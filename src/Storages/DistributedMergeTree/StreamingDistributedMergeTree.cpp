#include "StreamingDistributedMergeTree.h"
#include "StorageDistributedMergeTree.h"

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
/// #include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SelectQueryInfo.h>

#include <base/logger_useful.h>

namespace DB
{
StreamingDistributedMergeTree::StreamingDistributedMergeTree(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    ContextPtr context_,
    StreamingFunctionDescriptionPtr streaming_func_desc_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , streaming_func_desc(std::move(streaming_func_desc_))
    , storage(DatabaseCatalog::instance().getTable(id_, context_))
    , log(&Poco::Logger::get(id_.getNameForLogs()))
{
    assert(streaming_func_desc);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum StreamingDistributedMergeTree::getQueryProcessingStage(
    ContextPtr /* context_ */,
    QueryProcessingStage::Enum /* to_stage */,
    const StorageMetadataPtr & /* metadata_snapshot */,
    SelectQueryInfo & query_info) const
{
    if (!query_info.syntax_analyzer_result->streaming)
    {
        throw Exception("Streaming table functions like HOP / TUMBLE only support streaming query for now", ErrorCodes::NOT_IMPLEMENTED);
    }
    return QueryProcessingStage::Enum::FetchColumns;
    /// return storage->getQueryProcessingStage(context_, to_stage, metadata_snapshot, query_info);
}

Pipe StreamingDistributedMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context_), BuildQueryPipelineSettings::fromContext(context_));
}

void StreamingDistributedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    unsigned num_streams)
{
    if (!query_info.syntax_analyzer_result->streaming)
    {
        throw Exception("Streaming table functions like HOP / TUMBLE only support streaming query for now", ErrorCodes::NOT_IMPLEMENTED);
    }

    auto distributed = storage->as<StorageDistributedMergeTree>();
    assert(distributed);
    distributed->readStreaming(
        query_plan,
        query_info,
        column_names,
        metadata_snapshot,
        context_,
        max_block_size,
        num_streams,
        streaming_func_desc);

    //// if (query_info.syntax_analyzer_result->streaming && query_info.syntax_analyzer_result->streaming_func_asts.contains(getStorageID()))
    /// if (query_info.syntax_analyzer_result->streaming)
    /// {
    /// }
    /// else
    /// {
    ///    storage->read(query_plan, column_names, metadata_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    /// }
}
}
