#include "LogManager.h"

#include <NativeLog/Base/Utils.h>
#include <NativeLog/MetaStore/MetaStore.h>

#include <base/logger_useful.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_LOCK_FILE;
    extern const int CANNOT_READ_FILE;
    extern const int INVALID_SETTING_VALUE;
    extern const int INVALID_STATE;
    extern const int LOG_DIR_UNAVAILABLE;
    extern const int LOG_ALREADY_EXISTS;
    extern const int LOG_NOT_EXISTS;
    extern const int CANNOT_LOAD_CONFIG;
}
}

namespace nlog
{
namespace
{
    template <typename K, typename KK, typename VV>
    bool
    insertToTwoLevelConcurrentHashMap(ConcurrentHashMap<K, ConcurrentHashMapPtr<KK, VV>> & table, const K & k, const KK & kk, const VV & vv)
    {
        ConcurrentHashMapPtr<KK, VV> inner;
        table.at(k, inner);
        if (inner)
        {
            auto [_, inserted] = inner->tryEmplace(kk, vv);
            return inserted;
        }
        else
        {
            auto [exist_inner, inserted] = table.tryEmplace(k, std::make_shared<ConcurrentHashMap<KK, VV>>());
            auto [_, inner_inserted] = exist_inner->tryEmplace(kk, vv);
            (void)inserted;
            return inner_inserted;
        }
    }

    template <typename K, typename KK, typename VV>
    bool insertOrAssignToTwoLevelConcurrentHashMap(
        ConcurrentHashMap<K, ConcurrentHashMapPtr<KK, VV>> & table, const K & k, const KK & kk, const VV & vv)
    {
        ConcurrentHashMapPtr<KK, VV> inner;
        table.at(k, inner);
        if (inner)
        {
            auto [_, inserted] = inner->insertOrAssign(kk, vv);
            return inserted;
        }
        else
        {
            auto [exist_inner, inserted] = table.tryEmplace(k, std::make_shared<ConcurrentHashMapPtr<KK, VV>>());
            auto [_, inner_inserted] = exist_inner->insertOrAssign(kk, vv);
            (void)inserted;
            return inner_inserted;
        }
    }

    template <typename K, typename KK, typename VV>
    bool getFromTwoLevelConcurrentHashMap(ConcurrentHashMap<K, ConcurrentHashMapPtr<KK, VV>> & table, const K & k, const KK & kk, VV & vv)
    {
        ConcurrentHashMapPtr<KK, VV> inner;
        table.at(k, inner);
        if (inner)
            return inner->at(kk, vv);

        return false;
    }

    template <typename K, typename KK, typename VV>
    bool eraseFromTwoLevelConcurrentHashMap(ConcurrentHashMap<K, ConcurrentHashMapPtr<KK, VV>> & table, const K & k, const KK & kk)
    {
        ConcurrentHashMapPtr<KK, VV> inner;
        table.at(k, inner);
        if (inner)
            return inner->erase(kk) == 1;

        return false;
    }
}

LogManager::LogManager(
    const std::vector<fs::path> & root_dirs_,
    const std::vector<fs::path> & initial_offline_dirs_,
    LogConfigPtr default_config_,
    const LogCompactorConfig & compactor_config_,
    const LogManagerConfig & log_manager_config_,
    MetaStore & meta_store_,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
    std::shared_ptr<ThreadPool> adhoc_scheduler_,
    TailCachePtr cache_)
    : root_dirs(root_dirs_)
    , default_config(std::move(default_config_))
    , compactor_config(compactor_config_)
    , log_manager_config(log_manager_config_)
    , meta_store(meta_store_)
    , scheduler(std::move(scheduler_))
    , adhoc_scheduler(std::move(adhoc_scheduler_))
    , cache(std::move(cache_))
    , logger(&Poco::Logger::get("LogManager"))
{
    createAndValidateLogDirs(initial_offline_dirs_);

    const auto & live_root_dirs = liveLogDirs();
    lockLogDirs(live_root_dirs);
    initCheckpoints(live_root_dirs);
}

LogManager::~LogManager()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, "Failed to shutdown");
    }

    for (auto & dir_lock : dir_locks)
        dir_lock->unlock();
}

void LogManager::startup()
{
    /// Make a copy of config since it can be changed
    auto config{default_config};
    startupWithConfigOverrides(config, fetchAllStreamConfigOverrides(config));
}

void LogManager::shutdown()
{
    if (stopped.test_and_set())
        /// Already shutdown
        return;

    LOG_INFO(logger, "Shutting down");

    /// Stop the log compactor first
    if (compactor)
        compactor->shutdown();

    auto all_live_log_dirs{liveLogDirs()};

    /// A root log dir can have multiple namespace dirs and there is only on thread pool for it
    std::vector<std::pair<fs::path, std::shared_ptr<ThreadPool>>> thread_pools;

    thread_pools.reserve(all_live_log_dirs.size());

    /// We only flush live log dirs
    for (const auto & root_dir : all_live_log_dirs)
    {
        LOG_INFO(logger, "Flushing and closing logs in {}", root_dir.c_str());
        auto pool = std::make_shared<ThreadPool>(
            log_manager_config.recovery_threads_per_data_dir, log_manager_config.recovery_threads_per_data_dir, 10000);

        thread_pools.emplace_back(root_dir, pool);

        auto logs{getLogs(root_dir)};
        for (auto & ns_logs : logs)
            for (auto & log : ns_logs.second)
                pool->scheduleOrThrow([log]() {
                    log->flush();
                    log->close();
                });
    }

    for (auto & dir_pool : thread_pools)
    {
        dir_pool.second->wait();

        /// Only checkpoint recovery sn changed logs
        checkpointRecoverySequencesInDir(dir_pool.first, getLogs(dir_pool.first, "", [](const auto & stream_shard_log) {
                                             return stream_shard_log.second->needCheckpointRecoveryPoint();
                                         }));

        /// Only checkpoint start sn changed logs
        checkpointLogStartSequencesInDir(dir_pool.first, getLogs(dir_pool.first, "", [](const auto & stream_shard_log) {
                                             return stream_shard_log.second->needCheckpointStartSequence();
                                         }));

        /// Mark that the shutdown was clean by creating marker file
        auto clean_shutdown_file = dir_pool.first / CLEAN_SHUTDOWN_FILE;
        /// Ignore the creation status, FIXME move to rocksdb
        ::open(clean_shutdown_file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    }

    LOG_INFO(logger, "Shutdown completed");
}

void LogManager::startupWithConfigOverrides(LogConfigPtr default_config_, const LogConfigMap & log_config_overries)
{
    /// This could take a while if shutdown was not clean
    loadLogs(default_config_, log_config_overries);

    if (scheduler)
    {
        /// Schedule the cleanup task to delete old logs
        log_retention_task = scheduler->createTask(LOGGER_NAME, [this]() { cleanupLogs(); });
        log_flush_task = scheduler->createTask(LOGGER_NAME, [this]() { flushDirtyLogs(); });
        log_recovery_point_checkpoint_task = scheduler->createTask(LOGGER_NAME, [this]() { checkpointLogRecoverySequences(); });
        log_start_sn_checkpoint_task = scheduler->createTask(LOGGER_NAME, [this]() { checkpointLogStartSequences(); });
        log_delete_task = scheduler->createTask(LOGGER_NAME, [this]() { removeLogs(); });

        log_retention_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
        log_flush_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
        log_recovery_point_checkpoint_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
        log_start_sn_checkpoint_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
        log_delete_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
    }

    if (compactor_config.enable_compactor)
    {
        /// Enable log compaction
        compactor = std::make_shared<LogCompactor>(compactor_config, liveLogDirs(), current_logs);
        compactor->startup();
    }
}

LogManager::TwoLevelUnorderedMap<std::string, StreamShard, int64_t> LogManager::readRecoverySequenceCheckpoints(const fs::path & dir)
{
    auto iter = checkpoints.find(dir);
    if (iter != checkpoints.end())
    {
        try
        {
            return iter->second->readLogRecoveryPointSequences();
        }
        catch (...)
        {
            LOG_WARNING(
                logger, "Error occurred while reading recovery-point-sns for directory {}, resetting the recovery sn to 0", dir.c_str());
        }
    }
    else
        LOG_WARNING(
            logger,
            "Didn't find log root_dir occurred while reading recovery-point-sns for directory {}, resetting the recovery sn to "
            "0",
            dir.c_str());

    return {};
}

LogManager::TwoLevelUnorderedMap<std::string, StreamShard, int64_t> LogManager::readStartSequenceCheckpoints(const fs::path & dir)
{
    auto iter = checkpoints.find(dir);
    if (iter != checkpoints.end())
    {
        try
        {
            return iter->second->readLogStarSequences();
        }
        catch (...)
        {
            LOG_WARNING(
                logger, "Error occurred while reading log-start-sns for directory {}, resetting the log start sn to 0", dir.c_str());
        }
    }
    else
        LOG_WARNING(
            logger,
            "Didn't find log root_dir occurred while reading log-start-sns for directory {}, resetting the log start sn to 0",
            dir.c_str());

    return {};
}

/// Recover and load all logs in the given data directories
void LogManager::loadLogs(LogConfigPtr default_config_, const LogConfigMap & log_config_overrides)
{
    auto load_start = DB::MonotonicMilliseconds::now();

    auto all_live_log_dirs{liveLogDirs()};

    std::string log_dirs_str;
    std::for_each(all_live_log_dirs.begin(), all_live_log_dirs.end(), [&log_dirs_str](const auto & p) { log_dirs_str += p.c_str(); });

    LOG_INFO(logger, "Loading logs from log_dirs={}", log_dirs_str);

    std::vector<std::shared_ptr<ThreadPool>> thread_pools;
    int32_t total_logs = 0;

    /// For each top log directories, load all streams in each of them
    for (const auto & dir : all_live_log_dirs)
    {
        try
        {
            thread_pools.push_back(loadLogsInDir(dir, default_config_, log_config_overrides, total_logs));
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Failed to load logs in dir {}", dir.c_str()));
        }
    }

    for (auto & pool : thread_pools)
        pool->wait();

    LOG_INFO(logger, "Loaded {} logs in {}ms", total_logs, DB::MonotonicMilliseconds::now() - load_start);
}

/// The structure of a root log dir
/// -- ROOT LOG DIR
///  | --- <METADATA FILE>
///  | --- <namespace1>
///  | --- <namespace2>
///  | --- <namespace3>
///            | --- <stream1-shard1>
///            | --- <stream2-shard2>
///            | --- <stream3-shard3>
///                      | --- <segment1>
///                      | --- <segment1.index>
///                      | --- <segment2>
///                      | --- <segment3>
std::shared_ptr<ThreadPool> LogManager::loadLogsInDir(
    const fs::path & root_log_dir, LogConfigPtr default_config_, const LogConfigMap & log_config_overrides, int32_t & total_logs)
{
    bool had_clean_shutdown = false;
    auto clean_shutdown_file = root_log_dir / CLEAN_SHUTDOWN_FILE;
    if (fs::exists(clean_shutdown_file))
    {
        LOG_INFO(logger, "Skipping recovery for all logs in {} since clean shutdown file was found", root_log_dir.c_str());

        /// Delete the clean shutdown file, so that if the broker crashes while loading the log,
        /// it is considered a hard shutdown during next boost up.
        fs::remove(clean_shutdown_file);
        had_clean_shutdown = true;
    }
    else
        LOG_INFO(logger, "Attempting recovery for all logs in {} since no clean shutdown file was found", root_log_dir.c_str());

    /// FIXME live query recovery / log start sns directly
    auto recovery_sns = readRecoverySequenceCheckpoints(root_log_dir);
    auto log_start_sns = readStartSequenceCheckpoints(root_log_dir);

    auto pool = std::make_shared<ThreadPool>(
        log_manager_config.recovery_threads_per_data_dir, log_manager_config.recovery_threads_per_data_dir, 10000);

    for (const auto & ns_dir_entry : fs::directory_iterator{root_log_dir})
    {
        /// Skip any non-directory files
        if (!ns_dir_entry.is_directory())
            continue;

        std::string ns{ns_dir_entry.path().filename()};
        if (ns == CKPT_DIR_NAME)
            continue;

        auto ns_config_iter = log_config_overrides.find(ns);
        auto ns_recovery_point_iter = recovery_sns.find(ns);
        auto ns_start_sn_iter = log_start_sns.find(ns);

        for (const auto & stream_shard_dir_entry : fs::directory_iterator{ns_dir_entry.path()})
        {
            /// Skip any non-directory files
            if (!stream_shard_dir_entry.is_directory())
                continue;

            /// More error handling for non stream-shard dir
            auto stream_shard = Log::streamShardFrom(stream_shard_dir_entry.path());
            /// if (stream_shard.stream == Log::METADATA_STREAM())
            ///     continue;

            /// Make a copy of it since the following pool schedule needs a copy
            auto stream_shard_dir(stream_shard_dir_entry.path());

            /// Config overrides
            auto config{default_config_};
            if (ns_config_iter != log_config_overrides.end())
            {
                auto iter = ns_config_iter->second.find(stream_shard.stream);
                if (iter != ns_config_iter->second.end())
                {
                    /// File system dir name doesn't contain stream name
                    /// update it here
                    stream_shard.stream.name = iter->first.name;
                    config = iter->second;
                }
                else
                    LOG_ERROR(logger, "Failed to find log config for stream_shard={}", stream_shard.string());
            }

            /// Recovery point sn
            int64_t recovery_point = 0;
            if (ns_recovery_point_iter != recovery_sns.end())
            {
                auto iter = ns_recovery_point_iter->second.find(stream_shard);
                if (iter != ns_recovery_point_iter->second.end())
                    recovery_point = iter->second;
            }

            /// Start sn
            int64_t log_start_sn = 0;
            if (ns_start_sn_iter != log_start_sns.end())
            {
                auto iter = ns_start_sn_iter->second.find(stream_shard);
                if (iter != ns_start_sn_iter->second.end())
                    log_start_sn = iter->second;
            }

            total_logs += 1;
            pool->scheduleOrThrow([=, this]() {
                LOG_INFO(
                    logger,
                    "Loading log in {} in namespace {} with log_start_sn={} recovery_point_sn={}",
                    stream_shard_dir.c_str(),
                    ns,
                    log_start_sn,
                    recovery_point);

                LogPtr log;
                auto start_ms = DB::MonotonicMilliseconds::now();

                try
                {
                    log = loadLog(ns, stream_shard, stream_shard_dir, config, had_clean_shutdown, log_start_sn, recovery_point);
                }
                catch (...)
                {
                    /// FIXME, is it safe to exclude the root log dir if one stream-shard failed to load
                    DB::tryLogCurrentException(logger, fmt::format("Error while loading log dir {}", stream_shard_dir.c_str()));
                }
                auto elapsed = DB::MonotonicMilliseconds::now() - start_ms;

                LOG_INFO(
                    logger,
                    "Completed load log in {} in namespace {} with {} segments in {}ms",
                    stream_shard_dir.c_str(),
                    ns,
                    log->numberOfSegments(),
                    elapsed);
            });
        }
    }
    return pool;
}

LogPtr LogManager::loadLog(
    const std::string & ns,
    const StreamShard & stream_shard,
    const fs::path & log_dir,
    LogConfigPtr default_config_,
    bool had_clean_shutdown,
    int64_t log_start_sn,
    int64_t recovery_point)
{
    auto log = Log::create(
        stream_shard, log_dir, default_config_, had_clean_shutdown, log_start_sn, recovery_point, scheduler, adhoc_scheduler, cache);

    if (log_dir.extension().string() == Log::DELETE_DIR_SUFFIX())
    {
        addLogToBeDeleted(log);
    }
    else
    {
        bool inserted = true;
        if (log->isFuture())
            inserted = insertToTwoLevelConcurrentHashMap(future_logs, ns, stream_shard, log);
        else
            inserted = insertToTwoLevelConcurrentHashMap(current_logs, ns, stream_shard, log);

        if (!inserted)
        {
            if (log->isFuture())
                throw DB::Exception(DB::ErrorCodes::INVALID_STATE, "Duplicate log directory found: {}", log_dir.string());
            else
                throw DB::Exception(
                    DB::ErrorCodes::INVALID_STATE,
                    "Duplicate log directory for {} are found in {}. It is likely because log directory failure happened while broker was "
                    "replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two "
                    "directories for this shard. It is recommended to delete the shard in the log directory that is known to have failed "
                    "recently",
                    stream_shard.string(),
                    log_dir.string());
        }
    }
    return log;
}

LogManager::LogConfigMap LogManager::fetchAllStreamConfigOverrides(LogConfigPtr config)
{
    auto response{meta_store.listStreams("", ListStreamsRequest{""})};
    if (response.hasError())
        throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Failed to load all stream configurations", response.errString());

    LogConfigMap results;

    for (auto & stream_desc : response.streams)
    {
        auto new_config = config->clone();
        new_config->flush_interval_ms = stream_desc.flush_ms;
        new_config->flush_interval_records = stream_desc.flush_messages;
        new_config->retention_size = stream_desc.retention_bytes;
        new_config->retention_ms = stream_desc.retention_ms;
        new_config->codec = stream_desc.codec;

        results[stream_desc.ns][Stream{stream_desc.stream, stream_desc.id}] = std::move(new_config);
    }

    return results;
}

LogConfigPtr LogManager::fetchLogConfig(const std::string & ns, const Stream & stream)
{
    auto response{meta_store.listStreams(ns, ListStreamsRequest{stream.name})};
    if (response.hasError())
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_LOAD_CONFIG,
            "Failed to load configuration for stream={} in namespace={} error={}",
            stream.name,
            ns,
            response.errString());

    const auto & stream_desc = response.streams[0];
    LogConfigPtr new_config{default_config->clone()};
    new_config->flush_interval_ms = stream_desc.flush_ms;
    new_config->flush_interval_records = stream_desc.flush_messages;
    new_config->retention_size = stream_desc.retention_bytes;
    new_config->retention_ms = stream_desc.retention_ms;
    new_config->codec = stream_desc.codec;
    return new_config;
}

LogPtr LogManager::getOrCreateLog(const std::string & ns, const StreamShard & stream_shard, bool is_new, bool is_future)
{
    std::lock_guard<std::mutex> guard{log_creation_or_deletion_lock};

    auto log = getLog(ns, stream_shard, is_future);
    if (log)
        return log;

    if (!is_new && !offlineLogDirs().empty())
        throw DB::Exception(
            DB::ErrorCodes::LOG_DIR_UNAVAILABLE, "Cannot create log for {} because log directories are offline", stream_shard.string());

    /// Create the log if it has not already been created in another thread
    fs::path log_dir;
    auto exist = getFromTwoLevelConcurrentHashMap(preferred_log_dirs, ns, stream_shard, log_dir);
    if (is_future)
    {
        if (!exist)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOG_DIR_UNAVAILABLE,
                "Cannot create the future log for {} without having a preferred log directory",
                stream_shard.string());
        }
        else
        {
            log = getLog(ns, stream_shard);
            if (log && log->parentDir() == log_dir)
                throw DB::Exception(
                    DB::ErrorCodes::LOG_ALREADY_EXISTS,
                    "Cannot create the future log for {} int the current log directory of this shard",
                    stream_shard.string());
        }
    }

    if (log_dir.empty())
        log_dir = nextLogDir();

    std::string log_dir_name;
    if (is_future)
        log_dir_name = Log::logFutureDirName(stream_shard);
    else
        log_dir_name = Log::logDirName(stream_shard);

    log_dir = createLogDirectory(log_dir, ns, log_dir_name);

    auto config = fetchLogConfig(ns, stream_shard.stream);
    log = Log::create(
        stream_shard,
        log_dir,
        config,
        /*has_clean_shutdown*/ true,
        /*log_start_sn*/ 0,
        /*recovery_point*/ 0,
        scheduler,
        adhoc_scheduler,
        cache);

    bool result = false;
    if (is_future)
        result = insertToTwoLevelConcurrentHashMap(future_logs, ns, stream_shard, log);
    else
        result = insertToTwoLevelConcurrentHashMap(current_logs, ns, stream_shard, log);

    assert(result);
    (void)result;

    LOG_INFO(logger, "Created log for shard={} in {} with properties={}", stream_shard.string(), log_dir.string(), config->string());

    /// Removed the preferred log dir since it has already been satisfied
    eraseFromTwoLevelConcurrentHashMap(preferred_log_dirs, ns, stream_shard);
    return log;
}

/// Get the log if it exists, otherwise return nullptr
LogPtr LogManager::getLog(const std::string & ns, const StreamShard & stream_shard, bool is_future)
{
    LogPtr res;
    if (is_future)
        getFromTwoLevelConcurrentHashMap(future_logs, ns, stream_shard, res);
    else
        getFromTwoLevelConcurrentHashMap(current_logs, ns, stream_shard, res);
    return res;
}

void LogManager::trim(const std::string & ns, const std::vector<StreamShardSequence> & shard_sns, bool is_future)
{
    std::unordered_set<fs::path> trimmed;

    for (const auto & tso : shard_sns)
    {
        LogPtr log;
        if (is_future)
            log = getLog(ns, tso.stream_shard, is_future);
        else
            log = getLog(ns, tso.stream_shard, is_future);

        if (!log)
            continue;

        /// May need to abort and pause the compacting of the log, and resume after truncation is done
        auto need_stop_compactor = tso.sn < log->activeSegment()->baseSequence();
        if (need_stop_compactor && !is_future)
            abortAndPauseCompaction(tso.stream_shard);

        try
        {
            if (log->trim(tso.sn))
                trimmed.insert(log->rootDir());

            if (need_stop_compactor && !is_future)
                maybeTrimCompactorCheckpointToActiveSegmentBaseSN(log, tso.stream_shard);
        }
        catch (...)
        {
        }

        if (need_stop_compactor && !is_future)
            resumeCompaction(tso.stream_shard);
    }

    for (const auto & root_dir : trimmed)
        checkpointRecoverySequencesInDir(root_dir, getLogs(root_dir, ns, [](const auto & stream_shard_log) {
                                             return stream_shard_log.second->needCheckpointRecoveryPoint();
                                         }));
}

/// Abort and pause cleaning of the provided shard and log a message about it
void LogManager::abortAndPauseCompaction(const StreamShard & stream_shard)
{
    if (compactor)
    {
        compactor->abortAndPause(stream_shard);
        LOG_INFO(logger, "The compacting for shard {} is aborted and paused", stream_shard.string());
    }
}

/// Truncate the compactor's checkpoint to the based sns of the active segment of
/// the provided log
void LogManager::maybeTrimCompactorCheckpointToActiveSegmentBaseSN(LogPtr log, const StreamShard & stream_shard)
{
    if (compactor)
        compactor->maybeTruncateCheckpoint(log->parentDir(), stream_shard, log->activeSegment()->baseSequence());
}

void LogManager::resumeCompaction(const StreamShard & stream_shard)
{
    if (compactor)
    {
        compactor->resume({stream_shard});
        LOG_INFO(logger, "Compacting for shard {} is resumed", stream_shard.string());
    }
}

/// log_dir shall be an absolute path
void LogManager::maybeUpdatePreferredLogDir(const std::string & ns, const StreamShard & stream_shard, const fs::path & log_dir)
{
    /// Don't cache the preferred log directory if either the current log or the future log
    /// for this shard exists in the specified log_dir
    for (auto is_future : {false, true})
    {
        auto log = getLog(ns, stream_shard, is_future);
        if (log && log->parentDir() == log_dir)
            return;
    }

    ConcurrentHashMapPtr<StreamShard, fs::path> stream_dirs;
    preferred_log_dirs.at(ns, stream_dirs);
    if (stream_dirs)
    {
        stream_dirs->insertOrAssign(stream_shard, log_dir);
    }
    else
    {
        auto [stream_dirs_map, _] = preferred_log_dirs.tryEmplace(ns, std::make_shared<ConcurrentHashMap<StreamShard, fs::path>>());
        stream_dirs_map->insertOrAssign(stream_shard, log_dir);
    }
}

std::vector<fs::path> LogManager::liveLogDirs() const
{
    if (live_log_dirs.size() == root_dirs.size())
        return root_dirs;

    return live_log_dirs.snap();
}

/// `dir` shall be an absolute path
bool LogManager::isLogDirOnline(const fs::path & dir) const
{
    bool found = false;
    for (const auto & log_dir : root_dirs)
    {
        if (fs::absolute(log_dir) == dir)
        {
            found = true;
            break;
        }
    }

    if (!found)
        return false;

    live_log_dirs.apply([&dir, &found](const auto & path) {
        if (fs::absolute(path) == dir)
            found = true;
    });
    return found;
}

/// Create and check validity of the given directories that are not in the given offline directories, specifically:
/// - Ensure that there are no duplicates in the directory list
/// - Create each directory if it doesn't exist
/// - Check that each path is a readable directory
void LogManager::createAndValidateLogDirs(const std::vector<fs::path> & initial_offline_dirs)
{
    std::unordered_set<fs::path> offline_set{initial_offline_dirs.begin(), initial_offline_dirs.end()};
    std::unordered_set<fs::path> canonical_paths;

    for (const auto & dir : root_dirs)
    {
        try
        {
            if (offline_set.contains(dir))
                throw DB::Exception(DB::ErrorCodes::LOG_DIR_UNAVAILABLE, "Failed to load {} during broker startup", dir.c_str());

            if (!fs::exists(dir))
            {
                LOG_INFO(logger, "Log directory {} not found, creating it", dir.string());
                fs::create_directories(dir);
                /// flushFile(dir, /*include_meta*/ true);
            }

            if (!fs::is_directory(dir) || !isReadable(fs::status(dir).permissions()))
                throw DB::Exception(DB::ErrorCodes::CANNOT_READ_FILE, "{} is not a readable log directory", dir.c_str());

            auto [_, inserted] = canonical_paths.insert(dir.lexically_normal());
            if (!inserted)
                throw DB::Exception(DB::ErrorCodes::INVALID_SETTING_VALUE, "Duplicate log directory found: {}", dir.c_str());
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Directory {} is offline", dir.c_str()));
        }

        live_log_dirs.add(dir);
    }

    if (live_log_dirs.empty())
    {
        LOG_FATAL(logger, "Empty log directories");
        throw DB::Exception(DB::ErrorCodes::INVALID_SETTING_VALUE, "Empty log directories");
    }
}

/// Calculate the number of shards in each directory and find the data directory with fewest shards
fs::path LogManager::nextLogDir() const
{
    /// Shortcut
    if (live_log_dirs.size() == 1)
    {
        fs::path dir;
        if (live_log_dirs.peek(dir))
            return dir;
    }

    /// Count the number of logs in each parent directory including 0 for empty directories
    std::unordered_map<fs::path, size_t> counts;

    for (auto * logs : {&current_logs, &future_logs})
    {
        auto current_ns_logs{logs->items()};
        for (const auto & ns_logs : current_ns_logs)
            ns_logs.second->apply([&counts](const auto & kv) { ++counts[kv.second->rootDir()]; });
    }

    /// zeros
    live_log_dirs.apply([&counts](const auto & dir) { counts[dir] = 0; });

    /// Choose the directory with least logs in it
    size_t min_count = std::numeric_limits<size_t>::max();
    const fs::path * min_path = nullptr;
    for (const auto & path_count : counts)
    {
        if (path_count.second < min_count)
        {
            min_path = &path_count.first;
            min_count = path_count.second;
        }
    }

    if (min_path)
        return *min_path;

    return {};
}

/// Lock all log directories
void LogManager::lockLogDirs(const std::vector<fs::path> & dirs)
{
    for (const auto & dir : dirs)
    {
        auto lock_file{dir};
        lock_file /= LOCK_EXT;

        try
        {
            FileLockPtr fl = std::make_shared<FileLock>(lock_file);
            if (!fl->tryLock())
                throw DB::Exception(DB::ErrorCodes::CANNOT_LOCK_FILE, "Cannot lock file {}", dir.c_str());

            dir_locks.push_back(std::move(fl));
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Cannot lock dir {}", dir.c_str()));
        }
    }
}

void LogManager::initCheckpoints(const std::vector<fs::path> & live_root_dirs)
{
    for (const auto & root_dir : live_root_dirs)
    {
        auto ckpt_dir{root_dir};
        ckpt_dir /= CKPT_DIR_NAME;

        checkpoints.emplace(root_dir, std::make_shared<Checkpoints>(ckpt_dir, logger));
    }
}

fs::path LogManager::createLogDirectory(const fs::path & log_dir, const std::string & ns, const std::string & log_dir_name)
{
    if (isLogDirOnline(log_dir))
    {
        auto full_log_dir = log_dir / ns / log_dir_name;
        try
        {
            fs::create_directories(full_log_dir);
            return full_log_dir;
        }
        catch (...)
        {
            LOG_ERROR(logger, "Failed to create log directories={}", full_log_dir.c_str());
            throw DB::Exception(
                DB::ErrorCodes::LOG_DIR_UNAVAILABLE,
                "Cannot create log {} because failed to create its directory {}",
                log_dir_name,
                full_log_dir.c_str());
        }
    }
    else
        throw DB::Exception(
            DB::ErrorCodes::LOG_DIR_UNAVAILABLE, "Cannot create log {} because log directory {} is offline", log_dir_name, log_dir.c_str());
}

std::vector<fs::path> LogManager::offlineLogDirs() const
{
    if (root_dirs.size() == live_log_dirs.size())
        return {};

    auto live_dirs = live_log_dirs.snap();
    std::unordered_set<fs::path> live_set{live_dirs.begin(), live_dirs.end()};

    std::vector<fs::path> offlines;

    assert(root_dirs.size() >= live_dirs.size());
    offlines.reserve(root_dirs.size() - live_dirs.size());
    for (const auto & dir : root_dirs)
        if (!live_set.contains(dir))
            offlines.push_back(dir);
    return offlines;
}

std::unordered_map<std::string, std::vector<LogPtr>> LogManager::getLogs(
    const fs::path & root_dir, const std::string & ns, std::function<bool(const std::pair<StreamShard, LogPtr> &)> predicate) const
{
    assert(!root_dir.empty());
    std::unordered_map<std::string, std::vector<LogPtr>> results;

    /// Step 1: filter according to namespace
    std::unordered_map<std::string, std::vector<ConcurrentHashMapPtr<StreamShard, LogPtr>>> logs;
    current_logs.apply([&](const auto & kv) {
        if (!ns.empty())
        {
            if (kv.first == ns)
                logs[ns].push_back(kv.second);
        }
        else
            logs[kv.first].push_back(kv.second);
    });

    /// Step 2: filter according to root_dir and predicate
    for (const auto & ns_logs : logs)
    {
        for (const auto & shard_logs : ns_logs.second)
            shard_logs->apply([&](const auto & stream_shard_log) {
                if (stream_shard_log.second->rootDir() != root_dir)
                    return;

                if (predicate && !predicate(stream_shard_log))
                    return;

                results[ns_logs.first].push_back(stream_shard_log.second);
            });
    }

    return results;
}

/// Delete any eligible logs. Only consider logs that are not compacted.
void LogManager::cleanupLogs()
{
    LOG_INFO(logger, "Beginning log cleanup...");

    size_t total = 0;
    auto start = DB::MonotonicMilliseconds::now();

    std::vector<std::pair<std::string, std::pair<StreamShard, LogPtr>>> deletable_logs;

    /// Collect all non-compact logs
    current_logs.apply([&](const auto & ns_logs) {
        ns_logs.second->apply([&](const auto & shard_log) {
            if (!shard_log.second->config().compact())
                deletable_logs.push_back({ns_logs.first, shard_log});
        });
    });

    try
    {
        for (auto & ns_log : deletable_logs)
        {
            LOG_DEBUG(logger, "Garbage collecting log={} in namespace={}", ns_log.first, ns_log.second.first.string());
            total += ns_log.second.second->deleteOldSegments();

            /// FIXME, delete future logs
            ConcurrentHashMapPtr<StreamShard, LogPtr> ns_future_logs;
            if (future_logs.at(ns_log.first, ns_future_logs))
            {
                LogPtr future_log;
                if (ns_future_logs->at(ns_log.second.first, future_log))
                {
                    assert(future_log);
                    LOG_DEBUG(logger, "Garbage collecting log={} in namespace={}", ns_log.first, ns_log.second.first.string());
                    total += future_log->deleteOldSegments();
                }
            }
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, "Fail to retention logs");
    }

    LOG_INFO(logger, "Log cleanup completed: {} files were deleted in {} milliseconds", total, DB::MonotonicMilliseconds::now() - start);

    if (!stopped.test())
        log_retention_task->scheduleAfter(log_manager_config.retention_check_ms);
}

/// Flush any log which has exceeded its flush interval and has unwritten messages
void LogManager::flushDirtyLogs()
{
    LOG_INFO(logger, "Beginning flush dirty logs...");

    auto all_current_logs{current_logs.items()};

    for (auto & [ns, ns_logs] : all_current_logs)
    {
        auto stream_logs{ns_logs->items()};
        for (auto & [stream_shard, log] : stream_logs)
        {
            const auto & config = log->config();
            try
            {
                auto elapsed = DB::UTCMilliseconds::now() - log->lastFlushed();
                LOG_DEBUG(
                    logger,
                    "Checking if flush is needed on {} in namespace {}, flush interval {}, last flushed {}, {} ms passed since last flush",
                    stream_shard.string(),
                    ns,
                    config.flush_interval_ms,
                    log->lastFlushed(),
                    elapsed);

                if (elapsed >= config.flush_interval_ms)
                    log->flush();
            }
            catch (...)
            {
                DB::tryLogCurrentException(logger, fmt::format("Failed to flush stream shard {}", stream_shard.string()));
            }
        }
    }

    if (!stopped.test())
        log_flush_task->scheduleAfter(log_manager_config.flush_recovery_sn_checkpoint_ms);
}

/// Write out the current recovery point for all logs to a text file in the log directory
/// to avoid recovering the whole log on startup
void LogManager::checkpointLogRecoverySequences()
{
    LOG_INFO(logger, "Beginning checkpoint log recovery sns...");

    auto all_live_log_dirs{liveLogDirs()};
    for (const auto & log_dir : all_live_log_dirs)
        /// Only checkpoint recovery sn changed logs
        checkpointRecoverySequencesInDir(log_dir, getLogs(log_dir, "", [](const auto & stream_shard_log) {
                                             return stream_shard_log.second->needCheckpointRecoveryPoint();
                                         }));

    /// We will need make sure all exceptions are handled, otherwise the reschedule will not be called.

    if (!stopped.test())
        log_recovery_point_checkpoint_task->scheduleAfter(log_manager_config.flush_recovery_sn_checkpoint_ms);
}

/// Write out the current log start sn for all logs to a text file in the log directory
/// to avoid exposing data that have been deleted by `DeleteRecordsRequest`.
void LogManager::checkpointLogStartSequences()
{
    LOG_INFO(logger, "Beginning checkpoint log start sns...");

    auto all_live_log_dirs{liveLogDirs()};
    for (const auto & log_dir : all_live_log_dirs)
        /// Only checkpoint start sn changed logs
        checkpointLogStartSequencesInDir(log_dir, getLogs(log_dir, "", [](const auto & stream_shard_log) {
                                             return stream_shard_log.second->needCheckpointStartSequence();
                                         }));

    /// We will need make sure all exceptions are handled, otherwise the reschedule will not be called.
    if (!stopped.test())
        log_start_sn_checkpoint_task->scheduleAfter(log_manager_config.flush_start_sn_checkpoint_ms);
}

/// Checkpoint recovery sns for all the provided logs
void LogManager::checkpointRecoverySequencesInDir(
    const fs::path & root_dir, const std::unordered_map<std::string, std::vector<LogPtr>> & logs_to_checkpoint)
{
    std::unordered_map<std::string, std::vector<StreamShardSequence>> sns;
    sns.reserve(logs_to_checkpoint.size());

    for (const auto & ns_logs : logs_to_checkpoint)
        for (const auto & log : ns_logs.second)
        {
            auto recovery_point = log->recoveryPoint();
            sns[ns_logs.first].emplace_back(log->streamShard(), recovery_point);
            log->beginCheckpointRecoveryPoint(recovery_point);
        }

    auto iter = checkpoints.find(root_dir);
    assert(iter != checkpoints.end());

    try
    {
        iter->second->updateLogRecoveryPointSequences(sns);

        /// Update the cache to reflect that we already checkpoint the recovery points in persistent store
        for (const auto & ns_logs : logs_to_checkpoint)
            for (const auto & log : ns_logs.second)
            {
                log->endCheckpointRecoveryPoint();
            }
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, fmt::format("Failed to checkpoint recovery sns in dir {}", root_dir.c_str()));
    }
}

/// Checkpoint log start sns for all the provided logs in the provided directory
void LogManager::checkpointLogStartSequencesInDir(
    const fs::path & root_dir, const std::unordered_map<std::string, std::vector<LogPtr>> & logs_to_checkpoint)
{
    std::unordered_map<std::string, std::vector<StreamShardSequence>> sns;
    sns.reserve(logs_to_checkpoint.size());

    for (const auto & ns_logs : logs_to_checkpoint)
    {
        for (const auto & log : ns_logs.second)
        {
            auto log_start_sn = log->logStartSequence();
            sns[ns_logs.first].emplace_back(log->streamShard(), log_start_sn);
            log->beginCheckpointStartSequence(log_start_sn);
        }
    }

    auto iter = checkpoints.find(root_dir);
    assert(iter != checkpoints.end());

    try
    {
        iter->second->updateLogStartSequences(sns);

        /// Update the cache to reflect that we already checkpoint the start sns in persistent store
        for (const auto & ns_logs : logs_to_checkpoint)
            for (const auto & log : ns_logs.second)
                log->endCheckpointStartSequence();
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, fmt::format("Failed to checkpoint log start sns in dir {}", root_dir.c_str()));
    }
}

/// Delete logs marked for deletion. Delete all logs for which `default_log.file_delete_delay_ms`
/// has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will
/// be considered for deletion in the next iteration of `removeLogs`. The next iteration will be executed
/// after the remaining time for the first log that is not deleted. If there are no more `logs_to_be_deleted`,
/// `removeLogs` will be executed after `default_log.file_delete_delay_ms`.
void LogManager::removeLogs()
{
    LOG_INFO(logger, "Beginning delete logs...");

    auto file_delete_delay = default_config->file_delete_delay_ms;
    auto next_delete_delay = [this, file_delete_delay]() {
        if (!logs_to_be_deleted.empty())
        {
            std::pair<LogPtr, int64_t> res;
            auto got_one = logs_to_be_deleted.peek(res);
            if (got_one)
                return res.second + file_delete_delay - DB::MonotonicMilliseconds::now();
            else
                return file_delete_delay;
        }
        else
            return file_delete_delay;
    };

    int64_t next_delay_ms = 0;
    while (true)
    {
        next_delay_ms = next_delete_delay();
        if (next_delay_ms > 0)
            break;

        std::pair<LogPtr, int64_t> res;
        auto got_one = logs_to_be_deleted.take(res);
        if (got_one)
        {
            try
            {
                res.first->remove();
                LOG_INFO(logger, "Deleted log for {} in {}", res.first->streamShard().string(), res.first->logDir().c_str());
            }
            catch (...)
            {
                DB::tryLogCurrentException(
                    logger,
                    fmt::format("Exception while deleting {} in dir {}", res.first->streamShard().string(), res.first->logDir().c_str()));
            }
        }
    }

    /// We will need make sure all exceptions are handled, otherwise the reschedule will not be called.
    if (!stopped.test())
        log_delete_task->scheduleAfter(next_delay_ms);
}

void LogManager::remove(const std::string & ns, const std::vector<StreamShard> & stream_shards)
{
    std::unordered_set<fs::path> log_dirs_to_remove;

    for (const auto & stream_shard : stream_shards)
    {
        cache->remove(stream_shard);
        try
        {
            auto log = getLog(ns, stream_shard);
            if (log)
            {
                log_dirs_to_remove.insert(log->rootDir());
                remove(ns, stream_shard, /*is_future*/ false, /*checkpoint*/ false);
            }
            else if (log = getLog(ns, stream_shard, true); log)
            {
                log_dirs_to_remove.insert(log->rootDir());
                remove(ns, stream_shard, /*is_future*/ true, /*checkpoint*/ false);
            }
            else
                LOG_WARNING(logger, "Shard={} in namespace={} which is going to be removed is not found", stream_shard.string(), ns);
        }
        catch (...)
        {
            /// FIXME, accept an exception handler in `remove` ?
            DB::tryLogCurrentException(logger, "Exception happened while remove stream shards");
            throw;
        }
    }

    for (const auto & root_dir : log_dirs_to_remove)
    {
        if (compactor)
            compactor->updateCheckpoints(root_dir);

        removeSequenceCheckpoints(root_dir, ns, stream_shards[0].stream);
    }
}

LogPtr LogManager::remove(const std::string & ns, const StreamShard & stream_shard, bool is_future, bool checkpoint)
{
    LogPtr log;
    {
        std::lock_guard guard{log_creation_or_deletion_lock};
        auto * logs = &current_logs;
        if (is_future)
            logs = &future_logs;

        if (getFromTwoLevelConcurrentHashMap(*logs, ns, stream_shard, log))
            eraseFromTwoLevelConcurrentHashMap(*logs, ns, stream_shard);
    }

    if (log)
    {
        const auto & root_dir = log->rootDir();

        /// We need to wait until there is no more compacting task on the log to be deleted
        /// before actually deleting it
        if (compactor && !is_future)
        {
            compactor->abort(stream_shard);
            if (checkpoint)
                compactor->updateCheckpoints(root_dir, stream_shard);
        }

        log->renameDir(Log::logDeleteDirName(stream_shard));

        if (checkpoint)
            removeSequenceCheckpoints(root_dir, ns, stream_shard);

        addLogToBeDeleted(log);
        LOG_INFO(
            logger,
            "Log for shard={} in namespace={} is renamed to {} and it scheduled for deletion",
            stream_shard.string(),
            ns,
            log->logDir().c_str());
    }
    else
    {
        if (offlineLogDirs().empty())
            throw DB::Exception(
                DB::ErrorCodes::LOG_NOT_EXISTS,
                "Failed to remove log for {} because it may be in one of the offline directories",
                stream_shard.string());
    }
    return log;
}

void LogManager::updateConfig(
    const std::string & ns,
    const std::vector<StreamShard> & stream_shards,
    const std::map<std::string, int32_t> & flush_settings,
    const std::map<std::string, int64_t> & retention_settings)
{
    for (const auto & stream_shard : stream_shards)
    {
        auto log = getLog(ns, stream_shard);
        if (log)
        {
            log->updateConfig(flush_settings, retention_settings);
        }
        else if (log = getLog(ns, stream_shard, true); log)
        {
            log->updateConfig(flush_settings, retention_settings);
        }
        else
            LOG_WARNING(logger, "Shard={} in namespace={} is not found", stream_shard.string(), ns);
    }
}

void LogManager::removeSequenceCheckpoints(const fs::path & root_dir, const std::string & ns, const Stream & stream)
{
    auto iter = checkpoints.find(root_dir);
    assert(iter != checkpoints.end());
    iter->second->removeLogSequences(ns, stream);
}

void LogManager::removeSequenceCheckpoints(const fs::path & root_dir, const std::string & ns, const StreamShard & stream_shard)
{
    auto iter = checkpoints.find(root_dir);
    assert(iter != checkpoints.end());
    iter->second->removeLogSequences(ns, stream_shard);
}
}
