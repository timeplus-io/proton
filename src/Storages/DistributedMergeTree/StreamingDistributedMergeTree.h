#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/StreamingFunctionDescription.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
class ColumnsDescription;
struct StorageID;

/// StreamingDistributedMergeTree is pure in-memory representation
/// when a stream query is executed. It is for read-query only
class StreamingDistributedMergeTree final : public shared_ptr_helper<StreamingDistributedMergeTree>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StreamingDistributedMergeTree>;

public:
    ~StreamingDistributedMergeTree() override = default;

    String getName() const override { return "StreamingDistributedMergeTree"; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Names getAdditionalRequiredColumns() const;

    Names getRequiredColumnsForStreamingFunction() const { return streaming_func_desc->input_columns; }
    StreamingFunctionDescriptionPtr getStreamingFunctionDescription() const { return streaming_func_desc; }

    Names getRequiredColumnsForTimestampExpr() const
    {
        if (timestamp_func_desc)
            return timestamp_func_desc->input_columns;
        return {};
    }
    StreamingFunctionDescriptionPtr getTimestampFunctionDescription() const { return timestamp_func_desc; }

    const StoragePtr & getInnerStorage() const { return storage; }

private:
    StreamingDistributedMergeTree(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        StorageMetadataPtr underlying_storage_metadata_snapshot_,
        ContextPtr context_,
        StreamingFunctionDescriptionPtr streaming_func_desc_,
        StreamingFunctionDescriptionPtr timestamp_func_desc_);

    StorageMetadataPtr underlying_storage_metadata_snapshot;
    StreamingFunctionDescriptionPtr streaming_func_desc;
    StreamingFunctionDescriptionPtr timestamp_func_desc;
    StoragePtr storage;

    Poco::Logger * log;
};
}
