#pragma once

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageSnapshot.h>
#include <base/shared_ptr_helper.h>
#include <Common/MultiVersion.h>

namespace DB
{
class CompletedPipelineExecutor;
class InterpreterSelectWithUnionQuery;
struct ExtraBlock;

class StorageMaterializedView final : public shared_ptr_helper<StorageMaterializedView>, public IStorage, WithMutableContext
{
    friend struct shared_ptr_helper<StorageMaterializedView>;

public:
    ~StorageMaterializedView() override;

    std::string getName() const override { return "MaterializedView"; }

    bool isView() const override { return true; }

    void drop() override;
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    /// Use inner target storage to check Alter command
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    void renameInMemory(const StorageID & new_table_id) override;

    void startup() override;

    void shutdown() override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    void checkValid() const;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    /// Get constant pointer to storage settings.
    MergeTreeSettingsPtr getSettings() const;

private:
    /// Return true on success, false on failure
    void createInnerTable();
    void doCreateInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr context_);

    void buildBackgroundPipeline();
    void executeSelectPipeline();
    void executeBackgroundPipeline();
    void cancelBackgroundPipeline();

    void updateStorageSettings();

    void validateInnerQuery(const StorageInMemoryMetadata & storage_metadata, const ContextPtr & local_context) const;

private:
    Poco::Logger * log;

    /// Target table
    StorageID target_table_id = StorageID::createEmpty();
    bool has_inner_table = false;
    bool is_attach = false;
    bool is_virtual = false;

    std::atomic_flag shutdown_called;

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
        bool attach_,
        bool is_virtual_);
};

}
