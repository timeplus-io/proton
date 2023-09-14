#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Interpreters/Streaming/TimestampFunctionDescription_fwd.h>
#include <Interpreters/Streaming/WindowCommon.h>
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

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    TableFunctionDescriptionPtr getStreamingTableFunctionDescription() const { return table_func_desc; }

    /// Whether it reads data from streaming store or historical store
    bool isStreaming() const { return streaming; }

    /// Return WindowType::NONE, if it has no window func
    WindowType windowType() const;
    TableFunctionDescriptionPtr getStreamingWindowFunctionDescription() const;

    /// Whether has GlobalAggregation in subquery
    bool hasGlobalAggregation() const { return has_global_aggr; }

    bool isProxyingSubqueryOrView() const;

    bool isRemote() const override;
    bool supportsParallelInsert() const override;
    bool supportsIndexForIn() const override;
    bool supportsSubcolumns() const override;
    DataStreamSemanticEx dataStreamSemantic() const override;

    std::variant<StoragePtr, ASTPtr> getProxyStorageOrSubquery() const;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

private:
    void validateProxyChain() const;

    Names getRequiredColumnsForProxyStorage(const Names & column_names) const;

    void doRead(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    void buildStreamingFunctionQueryPlan(
        QueryPlan & query_plan,
        const Names & required_columns_after_streaming_window,
        const SelectQueryInfo & query_info,
        const StorageSnapshotPtr & storage_snapshot) const;

    void processChangelogStep(QueryPlan & query_plan, const Names & required_columns_after_streaming_window) const;

    void processDedupStep(QueryPlan & query_plan, const Names & required_columns_after_streaming_window) const;

    void processTimestampStep(QueryPlan & query_plan, const SelectQueryInfo & query_info) const;

    void processWindowAssignmentStep(
        QueryPlan & query_plan,
        const SelectQueryInfo & query_info,
        const Names & required_columns_after_streaming_window,
        const StorageSnapshotPtr & storage_snapshot) const;

    Names getRequiredInputs(Names required_outputs) const;

    Block checkAndGetOutputHeader(const Names & required_columns, const Block & input_header) const;

private:
    ProxyStream(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        ContextPtr context_,
        TableFunctionDescriptionPtr table_func_desc_,
        TimestampFunctionDescriptionPtr timestamp_func_desc_,
        StoragePtr nested_proxy_storage_,
        String internal_name_,
        StoragePtr storage_,
        ASTPtr subquery_,
        DataStreamSemanticEx data_stream_semantic_,
        bool streaming_ = false);

    TableFunctionDescriptionPtr table_func_desc;
    TimestampFunctionDescriptionPtr timestamp_func_desc;
    /// ProxyStream can wrap a ProxyStream
    StoragePtr nested_proxy_storage;
    String internal_name;

    /// This is the underlying storage: either a view or a physical stream
    StoragePtr storage;
    ASTPtr subquery;
    bool has_global_aggr = false;

    DataStreamSemanticEx data_stream_semantic;

    bool streaming = false;

    Poco::Logger * log;
};
}
}
