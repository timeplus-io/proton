#include "StreamingDistributedMergeTree.h"
#include "StorageDistributedMergeTree.h"

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>

#include <base/logger_useful.h>

namespace DB
{
StreamingDistributedMergeTree::StreamingDistributedMergeTree(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    StorageMetadataPtr underlying_storage_metadata_snapshot_,
    ContextPtr context_,
    StreamingFunctionDescriptionPtr streaming_func_desc_,
    StreamingFunctionDescriptionPtr timestamp_func_desc_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , underlying_storage_metadata_snapshot(underlying_storage_metadata_snapshot_)
    , streaming_func_desc(std::move(streaming_func_desc_))
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , storage(DatabaseCatalog::instance().getTable(id_, context_))
    , log(&Poco::Logger::get(id_.getNameForLogs()))
{
    assert(streaming_func_desc);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum StreamingDistributedMergeTree::getQueryProcessingStage(
    ContextPtr context_,
    QueryProcessingStage::Enum to_stage,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info) const
{
    return storage->getQueryProcessingStage(context_, to_stage, metadata_snapshot, query_info);
}

Pipe StreamingDistributedMergeTree::read(
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

void StreamingDistributedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & /* metadata_snapshot */,
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

    auto distributed = storage->as<StorageDistributedMergeTree>();
    assert(distributed);
    distributed->read(
        query_plan,
        updated_column_names,
        underlying_storage_metadata_snapshot,
        query_info,
        context_,
        processed_stage,
        max_block_size,
        num_streams);
}

Names StreamingDistributedMergeTree::getAdditionalRequiredColumns() const
{
    Names required;

    if (timestamp_func_desc)
        for (const auto & name : timestamp_func_desc->input_columns)
            required.push_back(name);

    for (const auto & name : streaming_func_desc->input_columns)
        if (name != STREAMING_TIMESTAMP_ALIAS)
            /// We remove the internal dependent ____ts column
            required.push_back(name);

    return required;
}

}
