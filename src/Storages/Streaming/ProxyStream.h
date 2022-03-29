#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
class ColumnsDescription;
struct StorageID;

/// StreamingStream is pure in-memory representation
/// when a stream query is executed. It is for read-query only
class ProxyStream final : public shared_ptr_helper<ProxyStream>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<ProxyStream>;

public:
    ~ProxyStream() override = default;

    String getName() const override { return "ProxyStream"; }

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

    NamesAndTypesList getVirtuals() const override;

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

    /// Whether it reads data from streaming store or historical store
    bool isStreaming() const { return streaming; }

    /// Whether has streaming func itself, i.e. tumble(...) or hop(...)
    bool hasStreamingFunc() const { return streaming_func_desc != nullptr; }

    /// Return WindowType::NONE, if it has no window func
    WindowType windowType() const
    {
        return streaming_func_desc != nullptr ? streaming_func_desc->type : WindowType::NONE;
    }

    /// Whether has GlobalAggregation in subquery
    bool hasGlobalAggregation() const { return has_global_aggr; }

    bool supportsSubcolumns() const override { return storage && storage->supportsSubcolumns(); }

private:
    ProxyStream(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        StorageMetadataPtr underlying_storage_metadata_snapshot_,
        ContextPtr context_,
        StreamingFunctionDescriptionPtr streaming_func_desc_,
        StreamingFunctionDescriptionPtr timestamp_func_desc_,
        ASTPtr subquery_ = nullptr,
        bool streaming_ = false);

    StorageMetadataPtr underlying_storage_metadata_snapshot;
    StreamingFunctionDescriptionPtr streaming_func_desc;
    StreamingFunctionDescriptionPtr timestamp_func_desc;
    StoragePtr storage;
    ASTPtr subquery;
    bool has_global_aggr = false;

    bool streaming = false;

    Poco::Logger * log;
};
}
