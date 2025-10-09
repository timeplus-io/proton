#pragma once

#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/LocalLog/Log/LogConfig.h>
#include <Cluster/LocalLog/Log/LogConfigMap.h>
#include <Cluster/LocalLog/Log/LogLoader.h>
#include <Cluster/LocalLog/Log/LogManagerConfig.h>

#include <Cluster/Base/Concurrent/ConcurrentHashMap.h>
#include <Cluster/Base/Concurrent/UnboundedQueue.h>
#include <Cluster/LocalLog/Base/FileLock.h>
#include <Cluster/LocalLog/Common/StreamShardSequence.h>

#include <base/ClockUtils.h>
#include <Common/BackgroundSchedulePool.h>

namespace cluster::nlog
{
/// The entry point of NativeLog management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
/// All read and write operations are delegated to the individual log instances
/// The log manager maintains logs in one or more directories. New logs are created in the data directory
/// with the fewest logs. No attempt is made to move partitions after the fact or balance based on size or I/O rate
/// A background thread handles log retention by periodically truncating excess log segments
/// LogManager is multi-thread safe

class LogManager final
{
public:
    /// \param root_dirs_ Root directories used to store logs. Root dirs are usually reside on different disk partitions
    /// \param config_ Default LogManagerConfig settings
    /// \param default_log_config_ Default LogConfig settings
    /// \param log_configs_ Current config for all logs during bootstrap
    LogManager(
        const std::vector<fs::path> & root_dirs_,
        LogManagerConfigPtr config_,
        LogConfigPtr default_log_config_,
        LogConfigMap log_configs_,
        DB::NLOG::BackgroundSchedulePool & scheduler_,
        ThreadPool & adhoc_scheduler_);

    ~LogManager();

    /// Start the background threads to flush logs and do log cleanup
    /// void startup(const std::unordered_map<std::string, std::vector<Stream>> & streams);
    void startup();

    /// Close all the logs
    void shutdown();

    /// If the log already exists, just return the existing log.
    /// \param stream_shard The shard whose log needs to be returned or created
    /// \param log_config The log configuration
    LogPtr getOrCreateLog(const StreamShard & stream_shard, LogConfigPtr log_config);

    /// Get the log if it exists, otherwise return nullptr
    LogPtr getLog(const StreamIDShard & stream_shard) const;

    /// Truncate the shard logs to the specified sns and checkpoint the recovery point to this sn
    /// \param shard_sns Shard logs that need to be truncated
    void trim(const std::vector<StreamShardSequence> & shard_sns);

    /// Rename the directories of the given stream-shards and add them in the queue for deletion
    /// Checkpoints are updated once all the directories have been renamed.
    /// This is an async process
    void remove(const std::vector<StreamShard> & stream_shards);

    void reconfigureDefaultLogConfig(LogConfigPtr log_config_) { default_log_config.swap(log_config_); }

    void updateConfig(
        const std::vector<StreamShard> & stream_shards,
        const std::unordered_map<std::string, int32_t> & flush_settings,
        const std::unordered_map<std::string, int64_t> & retention_settings);

    const LogConfig & currentDefaultConfig() const { return *default_log_config; }

    /// Get the log config if log exists, otherwise return nullptr
    nlog::LogConfigPtr getLogConfig(const StreamIDShard & stream_shard) const;

    size_t numStreamShards() const noexcept { return current_logs.size(); }


private:
    void startupWithConfigOverrides(const LogConfigMap & log_config_overrides);

    void loadLogs(const LogConfigMap & log_config_overrides);

    std::shared_ptr<ThreadPool>
    loadLogsInDir(const fs::path & dir, const LogConfigMap & log_config_overrides, std::atomic<size_t> & total_logs);

    LogPtr loadLog(const StreamShard & stream_shard, const fs::path & log_dir, LogConfigPtr default_config_);

    const fs::path & nextLogDir() const;

    void lockLogDirs(const std::vector<fs::path> & root_dirs);

    void createAndValidateLogDirs();
    fs::path createLogDirectory(const fs::path & root_dir, const std::string & ns, const std::string & log_dir_name);

    LogConfigPtr mergeDefaultConfig(LogConfigPtr config) const;

    /// Get logs search the logs according to root_dir, namespace and the predicate
    /// @root_dir Log root directory. It shall not be empty
    /// @ns Namespace in the log root directory. May be empty
    /// @predicate, Predicate which accepts a std::pair<StreamShard, LogPtr> and returns true means we need collect it,
    /// otherwise return false
    /// \return vector of Log
    std::vector<LogPtr>
    getLogs(const fs::path & root_dir, const std::string & ns = "", std::function<bool(const LogPtr &)> predicate = {}) const;

    void addLogToBeDeleted(LogPtr log) { logs_to_be_deleted.emplace(log, DB::MonotonicMilliseconds::now()); }

    /// Rename the directory of the given stream-shard `log_dir` as `log_dir.uuid.delete` and
    /// add it tin the queue for deletion
    /// This is an async process
    /// \return the removed log
    LogPtr remove(const StreamShard & stream_shard);

    void cleanupLogs();

    void flushDirtyLogs();

    void removeLogs();

    void writeCleanShutdownFile(const fs::path & dir) const;
    bool removeCleanShutdownFile(const fs::path & dir) const;

private:
    static const int32_t PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS = 10 * 60 * 1000;
    static const int32_t INITIAL_TASK_DELAY_MS = 30 * 1000;
    inline static const std::string LOCK_EXT = ".lock";
    inline static const std::string LOGGER_NAME = "LogManager";

    /// Clean shutdown file that indicates the NativeLog broker was cleanly shutdown.
    /// This is used to avoid unnecessary recovery after a clean shutdown. In theory this
    /// would be avoided by passing in the recovery point, however finding the correct position
    /// to do this requires accessing the sn index which may not be safe in an unclean shutdown
    inline static const std::string CLEAN_SHUTDOWN_FILE = ".nlog_cleanshutdown";

private:
    std::atomic_flag stopped;

    std::vector<fs::path> root_dirs;
    LogManagerConfigPtr config;
    LogConfigPtr default_log_config;

    /// Does the system have clean shutdown previously
    bool had_clean_shutdown_prev = false;

    ConcurrentHashMap<StreamIDShard, LogPtr> current_logs;

    /// Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion
    UnboundedQueue<std::pair<LogPtr, int64_t>> logs_to_be_deleted;

    std::vector<FileLockPtr> dir_locks;

    DB::NLOG::BackgroundSchedulePool & scheduler;
    ThreadPool & adhoc_scheduler;

    /// Background tasks
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_retention_task;
    /// DB::NLOG::BackgroundSchedulePool::TaskHolder log_flush_task;
    DB::NLOG::BackgroundSchedulePool::TaskHolder log_delete_task;

    LoggerPtr logger;
};
}
