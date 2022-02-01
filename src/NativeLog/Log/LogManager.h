#pragma once

#include "Log.h"
#include "LogCompactor.h"
#include "LogCompactorConfig.h"
#include "LogConfig.h"
#include "LogLoader.h"
#include "LogManagerConfig.h"

#include <NativeLog/Base/Concurrent/BlockingQueue.h>
#include <NativeLog/Base/Concurrent/ConcurrentHashMap.h>
#include <NativeLog/Base/Concurrent/UnboundedQueue.h>
#include <NativeLog/Base/FileLock.h>
#include <NativeLog/Base/Stds.h>
#include <NativeLog/Checkpoints/Checkpoints.h>
#include <NativeLog/Common/LogDirFailureChannel.h>
#include <NativeLog/Common/TopicShard.h>

#include <Common/BackgroundSchedulePool.h>
#include <Common/Exception.h>

namespace nlog
{
/// The entry point of NativeLog management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
/// All read and write operations are delegated to the individual log instances
/// The log manager maintains logs in one or more directories. New logs are created in the data directory
/// with the fewest logs. No attempt is made to move partitions after the fact or balance based on size or I/O rate
/// A background thread handles log retention by periodically truncating excess log segments
/// LogManager is multi-thread safe

class LogManager final : private boost::noncopyable
{
public:
    /// @param root_dirs_ Root directories used to store logs. Root dirs are usually reside on different disk partitions
    /// @param initial_offline_dirs_ Directories which are offline
    /// @param default_config_ Default LogConfig settings
    /// @param compactor_config_ Default CleanerConfig settings
    /// @param log_manager_config_ Default LogManagerConfig settings
    LogManager(
        const std::vector<fs::path> & root_dirs_,
        const std::vector<fs::path> & initial_offline_dirs_,
        LogConfigPtr default_config_,
        const LogCompactorConfig & compactor_config_,
        const LogManagerConfig & log_manager_config_,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
        std::shared_ptr<ThreadPool> adhoc_scheduler_);

    ~LogManager();

    /// Start the background threads to flush logs and do log cleanup
    void startup(const std::unordered_map<std::string, std::vector<Topic>> & topics);

    /// Close all the logs
    void shutdown();

    /// If the log already exists, just return the existing log
    /// Otherwise if is_new=true or if there is no offline log directory, create log for the given topic and the given shard
    /// Otherwise throw exception
    /// @param ns Namespace of the topic shard
    /// @param topic_shard The shard whose log needs to be returned or created
    /// @param is_new Whether the replica should have existed on the broker or not
    /// @param is_future True if the future log of the specified shard should be returned or created
    /// @param topic_id The topic ID of the shard's topic
    LogPtr getOrCreateLog(
        const std::string & ns, const TopicShard & topic_shard, bool is_new, bool is_future, const TopicID & topic_id);

    /// Get the log if it exists, otherwise return nullptr
    LogPtr getLog(const std::string & ns, const TopicShard & topic_shard, bool is_future = false);

    /// Truncate the shard logs to the specified offsets and checkpoint the recovery point to this offset
    /// @param ns Namespace of the topic shards
    /// @param shard_offsets Shard logs that need to be truncated
    /// @param is_future True iff the truncation should be performed on the future log of the specified shards
    void trim(const std::string & ns, const std::vector<TopicShardOffset> & shard_offsets, bool is_future);

    /// Rename the directories of the given topic-shards and add them in the queue for deletion
    /// Checkpoints are updated once all the directories have been renamed.
    /// This is an async process
    void remove(const std::string & ns, const std::vector<TopicShard> & topic_shards);

    /// log_dir shall be an absolute path
    void maybeUpdatePreferredLogDir(const std::string & ns, const TopicShard & topic_shard, const fs::path & log_dir);

    void reconfigureDefaultLogConfig(LogConfigPtr log_config_) { default_config = log_config_; }

    const LogConfig & currentDefaultConfig() const { return *default_config; }

private:
    using OffsetCheckpointFileMap = std::unordered_map<fs::path, CheckpointsPtr>;

    template <typename K, typename KK, typename VV>
    using TwoLevelUnorderedMap = std::unordered_map<K, std::unordered_map<KK, VV>>;

    /// ns -> topic -> config
    using LogConfigMap = TwoLevelUnorderedMap<std::string, Topic, LogConfigPtr>;

    void startupWithConfigOverrides(LogConfigPtr default_config_, const LogConfigMap & log_config_overrides);

    void loadLogs(LogConfigPtr default_config_, const LogConfigMap & log_config_overrides);

    std::shared_ptr<ThreadPool> loadLogsInDir(
        const fs::path & dir,
        LogConfigPtr default_config_,
        const LogConfigMap & log_config_overrides,
        int32_t & total_logs);

    LogPtr loadLog(
        const std::string & ns,
        const TopicID & topic_id,
        const fs::path & log_dir,
        LogConfigPtr default_config_,
        bool had_clean_shutdown,
        int64_t log_start_offset,
        int64_t recovery_point);

    LogConfigMap
    fetchTopicConfigOverrides(LogConfigPtr config, const std::unordered_map<std::string, std::vector<Topic>> & topics);

    void createAndValidateLogDirs(const std::vector<fs::path> & initial_offline_dirs);

    void abortAndPauseCompaction(const TopicShard & topic_shard);
    void resumeCompaction(const TopicShard & topic_shard);
    void maybeTrimCompactorCheckpointToActiveSegmentBaseOffset(LogPtr log, const TopicShard & topic_shard);

    fs::path nextLogDir() const;

    void lockLogDirs(const std::vector<fs::path> & root_dirs);

    void initCheckpoints(const std::vector<fs::path> & live_root_dirs);

    fs::path createLogDirectory(const fs::path & root_dir, const std::string & ns, const std::string & log_dir_name);

    LogConfigPtr fetchLogConfig(const std::string & ns, const std::string & topic);

    std::vector<fs::path> offlineLogDirs() const;

    std::vector<fs::path> liveLogDirs() const;

    /// `dir` shall be an absolute path
    bool isLogDirOnline(const fs::path & root_dir) const;

    /// Get logs search the logs according to root_dir, namespace and the predicate
    /// @root_dir Log root directory. It shall not be empty
    /// @ns Namespace in the log root directory. May be empty
    /// @predicate, Predicate which accepts a std::pair<TopicShard, LogPtr> and returns true means we need collect it,
    /// otherwise return false
    /// @return Namespace -> vector of Log
    std::unordered_map<std::string, std::vector<LogPtr>> getLogs(
        const fs::path & root_dir,
        const std::string & ns = "",
        std::function<bool(const std::pair<TopicShard, LogPtr> &)> predicate = {}) const;

    TwoLevelUnorderedMap<std::string, TopicShard, int64_t> readRecoveryOffsetCheckpoints(const fs::path & dir);
    TwoLevelUnorderedMap<std::string, TopicShard, int64_t> readStartOffsetCheckpoints(const fs::path & dir);

    void addLogToBeDeleted(LogPtr log) { logs_to_be_deleted.emplace(log, DB::MonotonicMilliseconds::now()); }

    /// Rename the directory of the given topic-partition `log_dir` as `log_dir.uuid.delete` and
    /// add it tin the queue for deletion
    /// This is an async process
    /// @return the removed log
    LogPtr remove(const std::string & ns, const TopicShard & topic_shard, bool is_future = false, bool checkpoint=true);

    void cleanupLogs();

    void flushDirtyLogs();

    void checkpointLogRecoveryOffsets();
    void checkpointRecoveryOffsetsInDir(const fs::path & root_dir, const std::unordered_map<std::string, std::vector<LogPtr>> & logs_to_checkpoint);

    void checkpointLogStartOffsets();
    void checkpointLogStartOffsetsInDir(const fs::path & root_dir, const std::unordered_map<std::string, std::vector<LogPtr>> & logs_to_checkpoint);

    /// Remove offset checkpoints for topic
    void removeOffsetCheckpoints(const fs::path & root_dir, const std::string & ns, const std::string & topic);
    void removeOffsetCheckpointsSingleTopicShard(const fs::path & root_dir, const std::string & ns, const TopicShard & topic_shard);

    void removeLogs();

private:
    static const int32_t PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS = 10 * 60 * 1000;
    static const int32_t INITIAL_TASK_DELAY_MS = 30 * 1000;
    inline static const std::string CKPT_DIR_NAME = "__ckpts";
    inline static const std::string LOCK_EXT = ".lock";
    inline static const std::string LOGGER_NAME = "LogManager";

    /// Clean shutdown file that indicates the NativeLog broker was cleanly shutdown.
    /// This is used to avoid unnecessary recovery after a clean shutdown. In theory this
    /// would be avoided by passing in the recovery point, however finding the correct position
    /// to do this requires accessing the offset index which may not be safe in an unclean shutdown
    inline static const std::string CLEAN_SHUTDOWN_FILE = ".nativelog_cleanshutdown";

private:
    std::vector<fs::path> root_dirs;
    LogConfigPtr default_config;
    LogCompactorConfig compactor_config;
    LogManagerConfig log_manager_config;

    template <typename K, typename KK, typename VV>
    using TwoLevelConcurrentHashMap = ConcurrentHashMap<K, ConcurrentHashMapPtr<KK, VV>>;

    TwoLevelConcurrentHashMap<std::string, TopicShard, LogPtr> current_logs;

    /// Future logs are put in the directory with "-future" suffix, Future log is created when user wants to
    /// move from one log directory to another log directory on the same broker. The directory of the future log
    /// will be renamed to replace the current log of the shard after the future log catches up with the current log
    TwoLevelConcurrentHashMap<std::string, TopicShard, LogPtr> future_logs;

    /// Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion
    /// BlockingQueue<std::pair<LogPtr, int64_t>> logs_to_be_deleted{std::numeric_limits<int32_t>::max()};
    UnboundedQueue<std::pair<LogPtr, int64_t>> logs_to_be_deleted;

    UnboundedQueue<fs::path> live_log_dirs;

    /// This map contains all shards whose logs are getting loaded and initialized. If log configuration
    /// of these partitions get updated at the same time, the corresponding entry in this map is set to `true`,
    /// which triggers a config reload after initialization is finished to get the latest config value.
    TwoLevelConcurrentHashMap<std::string, TopicShard, bool> shards_initializing;

    TwoLevelConcurrentHashMap<std::string, TopicShard, fs::path> preferred_log_dirs;

    OffsetCheckpointFileMap checkpoints;

    std::vector<FileLockPtr> dir_locks;

    std::mutex log_creation_or_deletion_lock;

    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;
    std::shared_ptr<ThreadPool> adhoc_scheduler;
    LogDirFailureChannelPtr log_dir_failure_channel;

    LogCompactorPtr compactor;

    /// Background tasks
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_retention_task;
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_flush_task;
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_recovery_point_checkpoint_task;
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_start_offset_checkpoint_task;
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_delete_task;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    Poco::Logger * logger;
};
}
