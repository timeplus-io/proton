#include "ProxyDistributedMergeTree.h"
#include "StorageDistributedMergeTree.h"

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages//Kafka/StorageKafka.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Common/ProtonCommon.h>

#include <base/logger_useful.h>

namespace DB
{
ProxyDistributedMergeTree::ProxyDistributedMergeTree(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    StorageMetadataPtr underlying_storage_metadata_snapshot_,
    ContextPtr context_,
    StreamingFunctionDescriptionPtr streaming_func_desc_,
    StreamingFunctionDescriptionPtr timestamp_func_desc_,
    ASTPtr subquery_,
    bool streaming_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , underlying_storage_metadata_snapshot(underlying_storage_metadata_snapshot_)
    , streaming_func_desc(std::move(streaming_func_desc_))
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , subquery(subquery_)
    , streaming(streaming_)
    , log(&Poco::Logger::get(id_.getNameForLogs()))
{
    if (streaming)
        assert(streaming_func_desc);

    if (!subquery)
    {
        storage = DatabaseCatalog::instance().getTable(id_, context_);
    }
    else
    {
        /// Whether has GlobalAggregation in subquery
        SelectQueryOptions options;
        auto interpreter_subquery
            = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context_, options.subquery());
        if (interpreter_subquery)
            has_global_aggr = interpreter_subquery->hasGlobalAggregation();
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum ProxyDistributedMergeTree::getQueryProcessingStage(
    ContextPtr context_,
    QueryProcessingStage::Enum to_stage,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info) const
{
    if (storage)
        return storage->getQueryProcessingStage(context_, to_stage, metadata_snapshot, query_info);
    else
        /// When it is created by subquery not a table
        return QueryProcessingStage::FetchColumns;
}

Pipe ProxyDistributedMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & /* metadata_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, underlying_storage_metadata_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context_), BuildQueryPipelineSettings::fromContext(context_));
}

void ProxyDistributedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// We drop STREAMING_WINDOW_START/END columns before forwarding the request
    Names updated_column_names;
    updated_column_names.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        if (column_name == STREAMING_WINDOW_START || column_name == STREAMING_WINDOW_END || column_name == STREAMING_TIMESTAMP_ALIAS)
            continue;

        updated_column_names.push_back(column_name);
    }

    /// If this storage is built from subquery
    if (!storage && subquery)
    {
        /// TODO: should we use a copy of context, instead of that from initial query?
        SelectQueryOptions options;
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], context_, options.subquery().noModify(), updated_column_names);
        if (interpreter_subquery)
        {
            interpreter_subquery->ignoreWithTotals();
            interpreter_subquery->buildQueryPlan(query_plan);
            //            query_plan.addInterpreterContext(context);
        }
        return;
    }

    if (auto * view = storage->as<StorageView>())
        return view->read(
            query_plan,
            updated_column_names,
            underlying_storage_metadata_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
    else if (auto * materialized_view = storage->as<StorageMaterializedView>())
        return materialized_view->read(
            query_plan,
            updated_column_names,
            underlying_storage_metadata_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
    else if (auto * kafka = storage->as<StorageKafka>())
        return kafka->read(
            query_plan,
            column_names,
            metadata_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);

    auto * distributed = storage->as<StorageDistributedMergeTree>();
    assert(distributed);

    if (streaming && context_->getSettingsRef().query_mode.value != "table")
        distributed->readStreaming(
            query_plan, query_info, updated_column_names, underlying_storage_metadata_snapshot, context_, max_block_size, num_streams);
    else
        distributed->readHistory(
            query_plan,
            updated_column_names,
            underlying_storage_metadata_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
}

NamesAndTypesList ProxyDistributedMergeTree::getVirtuals() const
{
    if (!storage)
        return {};

    auto * materialized_view = storage->as<StorageMaterializedView>();
    if (materialized_view)
        return materialized_view->getVirtuals();

    auto * distributed = storage->as<StorageDistributedMergeTree>();
    if (!distributed)
        return {};

    if (streaming)
        return distributed->getVirtuals();

    return distributed->getVirtualsHistory();
}

Names ProxyDistributedMergeTree::getAdditionalRequiredColumns() const
{
    Names required;

    if (timestamp_func_desc)
        for (const auto & name : timestamp_func_desc->input_columns)
            required.push_back(name);

    if (streaming_func_desc)
        for (const auto & name : streaming_func_desc->input_columns)
            if (name != STREAMING_TIMESTAMP_ALIAS)
                /// We remove the internal dependent __tp_ts column
                required.push_back(name);

    return required;
}

}
