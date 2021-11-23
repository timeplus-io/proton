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

    const Names & getRequiredColumnsForStreamingFunction() const { return streaming_func_desc->input_columns; }
    StreamingFunctionDescriptionPtr getStreamingFunctionDescription() const { return streaming_func_desc; }

    const Names & getRequiredColumnsForTimestampExpr() const { return timestamp_expr_required_columns; }
    ExpressionActionsPtr getTimestampExpr() const { return timestamp_expr; }
    const StoragePtr & getInnerStorage() const { return storage; }

private:
    StreamingDistributedMergeTree(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        ContextPtr context_,
        StreamingFunctionDescriptionPtr streaming_func_desc_,
        ExpressionActionsPtr timestamp_expr_,
        const Names & timestamp_expr_required_columns_
    );

    StreamingFunctionDescriptionPtr streaming_func_desc;
    ExpressionActionsPtr timestamp_expr;
    Names timestamp_expr_required_columns;
    StoragePtr storage;

    Poco::Logger * log;
};
}
