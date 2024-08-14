#pragma once

#include <Core/RecoveryPolicy.h>
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
struct BlockIO;
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

    void preDrop() override;
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
    Streaming::DataStreamSemanticEx dataStreamSemantic() const override;

    /// Get constant pointer to storage settings.
    MergeTreeSettingsPtr getSettings() const;

    bool isReady() const override;

    bool supportsStreamingQuery() const override { return true; }

private:
    /// Return true on success, false on failure
    void createInnerTable();
    void doCreateInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr context_);

    void initBackgroundState();
    BlockIO buildBackgroundPipeline(ContextMutablePtr local_context);
    void executeBackgroundPipeline(BlockIO & io, ContextMutablePtr local_context);

    void updateStorageSettingsAndTTLs();

    void validateInnerQuery(const StorageInMemoryMetadata & storage_metadata, const ContextPtr & local_context) const;

    void checkDependencies() const;

    template <typename Duration>
    void waitFor(Duration && duration);

private:
    Poco::Logger * log;

    /// Target table
    StorageID target_table_id = StorageID::createEmpty();
    bool has_inner_table = false;
    bool is_attach = false;
    bool is_virtual = false;

    std::atomic_flag shutdown_called;

    std::condition_variable wait_cv;
    std::mutex wait_cv_mutex;

    /// Background state
    struct State
    {
        Poco::Logger * log;
        ThreadFromGlobalPool thread;
        enum ThreadStatus
        {
            UNKNOWN = 0,
            CHECKING_DEPENDENCIES,
            BUILDING_PIPELINE,
            EXECUTING_PIPELINE,
            FATAL /// Always last one
        };
        std::atomic<ThreadStatus> thread_status;

        String err_msg;
        std::atomic_int32_t err; /// != 0, background thread has exception
        std::atomic_bool is_cancelled;

        RecoveryPolicy recovery_policy;

        ~State();
        void terminate();
        void setException(int code, const String & msg, bool log_error = true);
        void checkException(const String & msg_prefix = "") const;
        void updateStatus(State::ThreadStatus status);
        void waitStatusUntil(State::ThreadStatus target_status) const;
    } background_state;

    static constexpr auto recheck_dependencies_interval = std::chrono::milliseconds(200);
    static constexpr auto recover_interval = std::chrono::seconds(5);

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
