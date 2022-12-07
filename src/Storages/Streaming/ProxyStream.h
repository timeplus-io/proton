#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
class ColumnsDescription;
struct StorageID;

namespace Streaming
{
/// StreamingStream is pure in-memory representation
/// when a stream query is executed. It is for read-query only
class ProxyStream final : public shared_ptr_helper<ProxyStream>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<ProxyStream>;

public:
    ~ProxyStream() override = default;

    String getName() const override { return "ProxyStream"; }

    bool supportsFinal() const override
    {
        if (storage)
            return storage->supportsFinal();

        return false;
    }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    Names getAdditionalRequiredColumns() const;

    FunctionDescriptionPtr getStreamingFunctionDescription() const { return streaming_func_desc; }

    /// Whether it reads data from streaming store or historical store
    bool isStreaming() const { return streaming; }

    /// Whether has streaming func itself, i.e. tumble(...) or hop(...) or session(...)
    bool hasStreamingWindowFunc() const { return streaming_func_desc != nullptr && streaming_func_desc->type != WindowType::NONE; }

    /// Return WindowType::NONE, if it has no window func
    WindowType windowType() const { return streaming_func_desc != nullptr ? streaming_func_desc->type : WindowType::NONE; }

    /// Whether has GlobalAggregation in subquery
    bool hasGlobalAggregation() const { return has_global_aggr; }

    bool isRemote() const override;
    bool supportsParallelInsert() const override;
    bool supportsIndexForIn() const override;
    bool supportsSubcolumns() const override;

    StoragePtr getNestedStorage() const;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot) const override;

    void buildStreamingProcessingQueryPlan(
        QueryPlan & query_plan,
        const Names & required_columns_after_streaming_window,
        const SelectQueryInfo & select_info,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context_,
        bool need_watermark) const;

private:
    void validateProxyChain() const;

    bool
    processTimestampStep(QueryPlan & query_plan, const Names & required_columns_after_streaming_window, const ContextPtr & context_) const;

    void processWatermarkStep(QueryPlan & query_plan, const SelectQueryInfo & query_info, bool proc_time) const;

    void processSessionStep(QueryPlan & query_plan, const SelectQueryInfo & query_info) const;

    void processWindowAssignmentStep(
        QueryPlan & query_plan, const Names & required_columns_after_streaming_window, const StorageSnapshotPtr & storage_snapshot) const;

private:
    ProxyStream(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        ContextPtr context_,
        FunctionDescriptionPtr streaming_func_desc_,
        FunctionDescriptionPtr timestamp_func_desc_,
        StoragePtr nested_proxy_storage_,
        String internal_name_,
        ASTPtr subquery_ = nullptr,
        bool streaming_ = false);

    FunctionDescriptionPtr streaming_func_desc;
    FunctionDescriptionPtr timestamp_func_desc;
    /// ProxyStream can wrap a ProxyStream
    StoragePtr nested_proxy_storage;
    String internal_name;

    /// This is the underlying storage: either a view or a physical stream
    StoragePtr storage;
    ASTPtr subquery;
    bool has_global_aggr = false;

    bool streaming = false;

    Poco::Logger * log;
};
}
}
