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

#include <boost/smart_ptr/atomic_shared_ptr.hpp>

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

    const PipelineState & getPipelineState() const noexcept { return pipeline_state; }

    /// Pause/Resume/Abort/Recover the background pipeline execution
    /// \return false means that
    bool pause(bool sync);
    bool resume(bool sync);
    bool abort(bool sync);
    bool recover(bool sync);

private:
    /// Return true on success, false on failure
    void createInnerTable();
    void doCreateInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr context_);

    void initPipelineState();
    void backgroundPipelineMaterializing();

    BlockIO buildBackgroundPipeline(ContextMutablePtr local_context);
    void executeBackgroundPipeline(BlockIO & io, ContextMutablePtr local_context);
    void prepareRecoveryAfterException(const Exception & e);

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
    struct PipelineState
    {
        explicit PipelineState(Poco::Logger * logger_) : logger(logger_) { }

        ~PipelineState();

        void cancel() noexcept;
        bool isCanceled() const noexcept;
        void setException(int code, const String & msg);
        void checkException(const String & msg_prefix = "") const;
        void updateStatus(Status status);
        void waitStatusUntil(Status target_status) const;

        enum Status
        {
            /* Normal status */
            Initializing = 0,
            CheckingDependencies = 1,
            BuildingPipeline = 2,
            ExecutingPipeline = 3,

            /* Stillness Status */
            Fatal = 4,
            Paused = 5,
        };

        Poco::Logger * logger = nullptr;
        ThreadPool executor{1};
        ThreadPool shutdown_pool{1};

        std::atomic<Status> status;

        String err_msg;
        std::atomic_int32_t err = 0; /// != 0, background thread has exception
        std::atomic_flag is_cancelled;
        std::atomic_int64_t retry_times = 0;

        ContextMutablePtr query_context;
        boost::atomic_shared_ptr<BlockIO> io;

        /// Internal state
        ExecuteMode execute_mode;
        /// Keep the last SNs of the stream source during the last retries.
        std::deque<std::vector<Int64>> failed_sn_queue;
        std::vector<Int64> recover_sns;
        bool recovery_disabled = false;
    };

    PipelineState pipeline_state;

    static constexpr auto internal_recheck_interval = std::chrono::milliseconds(200);
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
