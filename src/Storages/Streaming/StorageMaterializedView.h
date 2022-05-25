#pragma once

#include <base/shared_ptr_helper.h>

#include <Common/MultiVersion.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST_fwd.h>

#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
class CompletedPipelineExecutor;
class InterpreterSelectQuery;
struct ExtraBlock;

class StorageMaterializedView final : public shared_ptr_helper<StorageMaterializedView>, public IStorage, WithMutableContext
{
    friend struct shared_ptr_helper<StorageMaterializedView>;
    friend class PushingToMaterializedViewMemorySink;
    friend class MaterializedViewMemorySource;

    using VirtualColumns = std::vector<std::tuple<String, DataTypePtr, std::function<Int64()>>>;

public:
    ~StorageMaterializedView() override;

    std::string getName() const override { return "MaterializedView"; }

    bool isView() const override { return true; }

    void drop() override;
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    void checkTableCanBeRenamed() const override;
    void renameInMemory(const StorageID & new_table_id) override;

    void startup() override;

    void shutdown() override;

    StoragePtr getTargetTable();

    void checkValid() const;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;
    bool isGlobalAggrInnerQuery() { return is_global_aggr_query; }

    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot) const override;

private:
    void initInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr context_);
    void buildBackgroundPipeline(InterpreterSelectQuery & inner_interpreter, const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr context_);
    void executeBackgroundPipeline();

private:
    Poco::Logger * log;

    /// Target table
    StorageID target_table_id = StorageID::createEmpty();
    StoragePtr target_table_storage = nullptr;
    bool is_attach = false;

    VirtualColumns virtual_columns;
    bool is_global_aggr_query = false;

    std::atomic_flag shutdown_called = ATOMIC_FLAG_INIT;

    /// In memory table
    using Data = std::deque<BlockPtr>;
    using DataConstIterator = typename Data::const_iterator;
    class InMemoryTable final
    {
    private:
        /// When the cached blocks is full (count or bytes), push one and pop one (FIFO)
        size_t max_blocks_count = 0;
        size_t max_blocks_bytes = 0;
        size_t total_blocks_bytes = 0;
        Data data;
        mutable std::shared_mutex mutex;

    public:
        explicit InMemoryTable(size_t max_blocks_count_, size_t max_blocks_bytes_);

        void write(Block && block);
        Data get() const;
    };
    std::unique_ptr<InMemoryTable> memory_table;

    /// Background update pipeline
    struct
    {
        std::atomic<bool> has_exception = false;
        std::exception_ptr exception;
    } background_status;
    PipelineExecutorPtr background_executor;
    QueryPipelineBuilder background_pipeline;
    ThreadFromGlobalPool background_thread;

protected:
    StorageMaterializedView(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);
};

}
