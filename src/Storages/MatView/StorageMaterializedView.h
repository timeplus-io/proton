#pragma once

#include <Checkpoint/CheckpointRequestMetrics.h>
#include <Checkpoint/CheckpointReplicationType.h>
#include <Checkpoint/LogStoreCheckpointContext.h>
#include <Cluster/Common/ExponentialBackoff.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/StreamShard.h>
#include <Core/ExecuteMode.h>
#include <Core/PathSize.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageSnapshot.h>

#include <base/shared_ptr_helper.h>

#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <magic_enum.hpp>

namespace DB
{
struct BlockIO;

/// \StorageMaterializedView contains 2 major components:
/// 1) A long running streaming query pipeline.
/// 2) An optional underlying inner stream which holds the materialized results
///    if there is no explicit target stream is provided.
///
/// - No distributed coordination or leader election
/// - Local filesystem-based checkpointing only
/// - Automatic recovery from checkpoint after crash/restart
class StorageMaterializedView final : public shared_ptr_helper<StorageMaterializedView>, public IStorage, WithMutableContext
{
private:
    friend struct shared_ptr_helper<StorageMaterializedView>;

    struct InitParams
    {
        bool attach = false;
        bool has_force_restore_data_flag = false;
        ASTPtr sharding_expr;
        String relative_data_path;
        std::unique_ptr<StreamSettings> storage_settings;
    };

    struct PipelineState
    {
        explicit PipelineState(StorageMaterializedView & mv_, LoggerPtr logger_)
            : mv(mv_), logger(logger_), metric_time(MonotonicMilliseconds::now())
        {
        }

        enum Status : uint8_t
        {
            None = 100,
            Initializing = 0,
            CheckingDependencies = 1,
            BuildingPipeline = 2,
            ExecutingPipeline = 3,

            Error = 4,
            /// Removed Suspended state (was 5)
            Paused = 6,

            /// Transient statuses
            AutoRecovering = 10, /// Auto recovering from exception
            Resuming = 11, /// Resuming from 'Paused' status
            Recovering = 12, /// Recovering from 'Error' status
        };

        bool active() const noexcept { return executor_tid != 0; }
        void shutdown() noexcept;
        void cancel(bool abort = true) noexcept;
        bool isCancelled() const noexcept;
        bool pipelineStopped() const noexcept;
        void setException(int code, String msg);
        void checkException(const String & msg_prefix = "") const;
        bool checkStatus(Status expected) const noexcept;
        void updateStatus(Status status) noexcept;
        void resetPipeline();

        String currentErrorMessage() const
        {
            if (err != 0)
            {
                std::scoped_lock lock{last_err_msg_mutex};
                chassert(!last_err_msg.empty());
                return last_err_msg;
            }
            else
            {
                return "";
            }
        }

        std::pair<String, Int64> lastErrorMessageAndTimestamp() const
        {
            std::scoped_lock lock{last_err_msg_mutex};
            return {last_err_msg, last_err_msg_ts};
        }

        /// \brief Compare the current status with the old status and update the status if they are the same
        /// \return true if the status is updated, otherwise false and the old_status is updated to the current status
        bool compareAndUpdateStatus(Status & old_status, Status new_status) noexcept;

        void waitStatusChanged(Status old_status) const;

        /// \brief Wait for the pipeline finish execution (or timeout)
        /// \return true if the pipeline is finished, false if the timeout is reached
        bool waitPipelineFinished(size_t max_wait_ms = 0) const;

        void waitFor(size_t wait_ms) const;

        StorageMaterializedView & mv;

        ThreadFromGlobalPool executor;
        std::atomic<uint64_t> executor_tid{0}; /// 0 means not running

        ContextMutablePtr query_context;
        boost::atomic_shared_ptr<BlockIO> io;
        mutable boost::atomic_shared_ptr<Streaming::StreamingSourceMetricsPtrs> last_streaming_source_metrics;
        bool can_recover_without_checkpoint = false;

        std::atomic<Status> status{Status::None};

    private:
        mutable std::mutex last_err_msg_mutex;
        Int64 last_err_msg_ts = 0;
        /// last_err_msg is never reset and maintains the last error message
        String last_err_msg;

    public:
        std::atomic_int32_t err = 0; /// != 0, background thread has exception
        std::atomic_flag is_cancelled;

        std::atomic_bool validated = false;
        mutable std::condition_variable wait_cv;
        mutable std::mutex wait_cv_mutex;

        LoggerPtr logger = nullptr;
        std::atomic_int64_t retry_times = 0;

        std::atomic_bool disable_checkpoint = false;
        std::atomic_bool enable_dlq = false;
        std::atomic_uint64_t dlq_max_message_batch_size = 0;
        std::atomic_uint64_t dlq_consecutive_failures_limit = 0;

        /// Internal state (Thread unsafe)
        ExecuteMode exec_mode;
        std::vector<Streaming::SequenceRange> continuously_dropped_sn_ranges;
        std::pair<int, uint32_t> failed_err_with_count{0, 0};
        std::vector<Int64> recover_sns;
        bool pause_on_start = false;

        int64_t pause_dlq_until{0};

        std::atomic<int64_t> metric_time;
        std::atomic<uint64_t> read_bytes;
        std::atomic<uint64_t> read_rows;
        std::atomic<uint64_t> written_bytes;
        std::atomic<uint64_t> written_rows;

        std::vector<std::string> processors_last_error_message;
    };

    struct Metrics
    {
        String status;
        std::pair<String, Int64> last_err_msg_and_ts;
        Int64 recover_times;
        Int64 memory_usage;
        UInt64 ckpt_storage_size;
        CheckpointRequestMetricsPtr ckpt_request_metrics;
        Streaming::StreamingSourceMetricsPtrs source_metrics;
    };

public:
    ~StorageMaterializedView() override;

    std::string getName() const override { return "MaterializedView"; }

    bool isView() const override { return true; }

    bool isLocal() const override { return false; }

    void drop() override;

    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    void onAlterQuery();
    void onAlterQuerySettings();

    void applyAlterCommandsToAllShards(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder) override;

    /// Use inner target storage to check Alter command
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void startup() override;

    void shutdown(bool dropping) override;

    const cluster::StreamShard & getCkptLogStreamShard() const noexcept { return ckpt_log_stream_shard; }

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;
    StorageMetadataPtr getTargetInMemoryMetadataPtr() const;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsSubcolumns() const override { return true; }
    bool supportsTTL() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;
    Streaming::DataStreamSemanticEx dataStreamSemantic() const override;

    bool isVirtualStorage() const override;

    /// Get target stream settings changes if the target is a Stream or KeyValueStream.
    /// Returns nullopt for external streams or when target is not available.
    std::optional<SettingsChanges> getTargetStreamSettingsChanges() const;

    bool isReady() const override;

    bool supportsStreamingQuery() const override { return true; }

    UInt32 getShards() const noexcept { return shards; }
    UInt32 getShardIDFromCheckpointShardID(UInt32 ckpt_shard) const;
    bool isCheckpointShardID(UInt32 shard) const noexcept;

    bool hasExternalTarget() const noexcept { return target_table_id.has_value(); }
    bool usesInnerStorage() const noexcept { return !target_table_id.has_value(); }

    bool backgroundPipelineActive() const noexcept { return pipeline_state.active(); }
    PipelineState::Status getPipelineStatus() const noexcept { return pipeline_state.status; }
    bool isRunning() const noexcept { return !pipeline_state.pipelineStopped(); }

    String currentErrorMessage() const { return pipeline_state.currentErrorMessage(); }
    std::pair<String, Int64> lastErrorMessageAndTimestamp() const { return pipeline_state.lastErrorMessageAndTimestamp(); }
    bool isExecutingNode() const noexcept { return true; }

    Int64 getRetryTimes() const noexcept { return pipeline_state.retry_times.load(); }

    const String & getInnerQueryId() const noexcept { return inner_query_id; }

    std::optional<StorageID> getExternalTargetTableID() const noexcept { return target_table_id; }

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<String> preferredColumn() const override;

    UInt64 getCheckpointSize() const;
    PathSizes getCheckpointStat() const;
    UInt64 getStorageSize() const override;
    Strings getDataPaths() const override;

    Int64 getMetricTime(bool reset = true) override;
    UInt64 readBytes(bool reset = true, bool external_ingress = false) override;
    UInt64 readRows(bool reset = true, bool external_ingress = false) override;
    UInt64 writtenBytes(bool reset = true, bool external_ingress = false) override;
    UInt64 writtenRows(bool reset = true, bool external_ingress = false) override;

    std::shared_ptr<Metrics> getMetrics() const;

    /// Pause/Resume/Abort/Recover the background pipeline execution
    /// \return error code
    int pause(int64_t timeout_ms);
    int resume();
    int abort();
    int recover();

    static std::shared_ptr<StorageMaterializedView> tryGetStorage(const StorageID & table_id, const ContextPtr & context_);

private:
    void initInnerStream();

    void validate(const StorageID & table_id_, const StorageInMemoryMetadata & metadata_, const ASTCreateQuery & query, bool attach_);

    void readRemote(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage);

    bool initPipeline(bool recovering);
    void runPipeline() noexcept;
    void shutdownPipeline();

    void initialize();
    void checkDependencies();
    void buildBackgroundPipeline();
    void executeBackgroundPipeline();
    void prepareRecoveryAfterException();

    /// \param can_recover_without_checkpoint - [output] true if the pipeline can be recovered without checkpoint
    QueryPipeline
    buildQueryPipelineImpl(ASTPtr inner_query, ContextMutablePtr query_context, bool * can_recover_without_checkpoint = nullptr) const;

    void syncInnerQueryContext(ContextMutablePtr query_context) const;

    void prepareCheckpoint();

    void handleDLQSettings();

    void validateInnerQuery(const StorageInMemoryMetadata & storage_metadata) const;

    void validateModifiedQuery(ASTPtr new_query) const;

    /// Flush the ckpt, then stop background pipeline execution, and finally switch to the specified status
    /// \return error code
    int flushAndStop(PipelineState::Status to_status, int64_t timeout_ms);

    void logToDLQ(const std::vector<std::shared_ptr<Streaming::ISource>> &);

    /// Internal Metrics methods
    Int64 getMemoryUsage() const;
    Streaming::StreamingSourceMetricsPtrs getStreamingSourceMetrics() const;
    CheckpointRequestMetricsPtr getCheckpointRequestMetrics() const;
    UInt64 getLogStoreDiskSize() const;

protected:
    StorageMaterializedView(
        std::unique_ptr<StreamSettings> storage_settings,
        const ASTPtr & sharding_expr,
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach_,
        ContextMutablePtr context_,
        bool has_force_restore_data_flag_);

private:
    std::atomic_flag stopped;
    std::atomic_flag started;

    std::unique_ptr<InitParams> init_params;

    UInt32 shards;

    cluster::NodeID this_node = cluster::Nulls::NullNodeID;

    String inner_query_id;

    /// Inner storage is the underlying inner stream which holds the query results of the Materialized View.
    /// It is nullptr if an external stream is provided during MV creation DDL. (INTO target_stream)
    StoragePtr inner_storage;

    /// \target_table_id is the storage ID of external target table
    std::optional<StorageID> target_table_id;

    LoggerPtr logger;

    /// This mutex prevents race conditions when multiple threads attempt to initialize or shutdown the pipeline_state concurrently.
    std::mutex pipeline_state_mutex;
    PipelineState pipeline_state;

    cluster::StreamShard ckpt_log_stream_shard;
    LogStoreCheckpointContextPtr logstore_ckpt_ctx;

    CheckpointContextPtr curr_ckpt_ctx;

    static constexpr size_t internal_recheck_interval_ms = 100;
    cluster::ExponentialBackoff recover_backoff_ms;
};

}
