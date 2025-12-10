#include <Cluster/Common/Constants.h>
#include <Cluster/LocalLog/Base/Utils.h>
#include <Cluster/LocalLog/Log/LogManager.h>

#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <fcntl.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

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

namespace cluster::nlog
{
LogManager::LogManager(
    const std::vector<fs::path> & root_dirs_,
    LogManagerConfigPtr config_,
    LogConfigPtr default_log_config_,
    LogConfigMap log_configs_,
    DB::NLOG::BackgroundSchedulePool & scheduler_,
    ThreadPool & adhoc_scheduler_)
    : root_dirs(root_dirs_)
    , config(config_)
    , default_log_config(std::move(default_log_config_))
    , scheduler(scheduler_)
    , adhoc_scheduler(adhoc_scheduler_)
    , logger(getLogger(LogManager::LOGGER_NAME))
{
    lockLogDirs(root_dirs);

    startupWithConfigOverrides(log_configs_);
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
    ///
}

void LogManager::shutdown()
{
    if (stopped.test_and_set())
        /// Already shutdown
        return;

    LOG_INFO(logger, "Shutting down");

    /// A root log dir can have multiple namespace dirs and there is only on thread pool for it
    std::vector<std::pair<fs::path, std::shared_ptr<ThreadPool>>> thread_pools;

    thread_pools.reserve(root_dirs.size());

    /// We only flush live log dirs
    for (const auto & root_dir : root_dirs)
    {
        LOG_INFO(logger, "Flushing and closing logs in {}", root_dir.c_str());
        auto pool = std::make_shared<ThreadPool>(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            config->recovery_threads_per_data_dir,
            config->recovery_threads_per_data_dir,
            10000,
            /*shutdown_on_exception=*/false);

        thread_pools.emplace_back(root_dir, pool);

        auto logs{getLogs(root_dir)};
        for (auto & log : logs)
            pool->scheduleOrThrow([log]() {
                setThreadName("LogFlusher");
                log->flush();
            });
    }

    for (auto & dir_pool : thread_pools)
    {
        dir_pool.second->wait();

        /// Mark that the shutdown was clean by creating marker file
        writeCleanShutdownFile(dir_pool.first);
    }

    log_retention_task->deactivate();
    /// log_flush_task->deactivate();
    log_delete_task->deactivate();

    LOG_INFO(logger, "Shutdown completed");
}

void LogManager::startupWithConfigOverrides(const LogConfigMap & log_config_overrides)
{
    /// This could take a while if shutdown was not clean
    loadLogs(log_config_overrides);

    /// Schedule the cleanup task to delete old logs
    log_retention_task = scheduler.createTask(LOGGER_NAME, [this]() { cleanupLogs(); });
    /// log_flush_task = scheduler.createTask(LOGGER_NAME, [this]() { flushDirtyLogs(); });
    log_delete_task = scheduler.createTask(LOGGER_NAME, [this]() { removeLogs(); });

    log_retention_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
    /// log_flush_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
    log_delete_task->scheduleAfter(INITIAL_TASK_DELAY_MS);
}

/// Recover and load all logs in the given data directories
void LogManager::loadLogs(const LogConfigMap & log_config_overrides)
{
    Stopwatch stopwatch;

    std::string log_dirs_str;
    std::for_each(root_dirs.begin(), root_dirs.end(), [&log_dirs_str](const auto & p) { log_dirs_str += p.c_str(); });

    LOG_INFO(logger, "Loading logs from log_dirs={}", log_dirs_str);

    std::vector<std::shared_ptr<ThreadPool>> thread_pools;

    std::atomic<size_t> total_logs = 0;

    /// For each top log directories, load all streams in each of them
    for (const auto & dir : root_dirs)
    {
        try
        {
            thread_pools.push_back(loadLogsInDir(dir, log_config_overrides, total_logs));
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Failed to load logs in dir {}", dir.c_str()));
        }
    }

    for (auto & pool : thread_pools)
        pool->wait();

    LOG_INFO(logger, "Loaded {} logs in {}ms", total_logs, stopwatch.elapsedMilliseconds());
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
std::shared_ptr<ThreadPool>
LogManager::loadLogsInDir(const fs::path & root_log_dir, const LogConfigMap & log_config_overrides, std::atomic<size_t> & total_logs)
{
    had_clean_shutdown_prev = removeCleanShutdownFile(root_log_dir);

    auto pool = std::make_shared<ThreadPool>(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        config->recovery_threads_per_data_dir,
        config->recovery_threads_per_data_dir,
        10000,
        /*shutdown_on_exception=*/false);

    for (const auto & ns_dir_entry : fs::directory_iterator{root_log_dir})
    {
        /// Skip any non-directory files
        if (!ns_dir_entry.is_directory())
            continue;

        std::string ns{ns_dir_entry.path().filename()};

        for (const auto & stream_shard_dir_entry : fs::directory_iterator{ns_dir_entry.path()})
        {
            /// Skip any non-directory files
            if (!stream_shard_dir_entry.is_directory())
                continue;

            /// Skip index, ckpt files
            auto filename = stream_shard_dir_entry.path().filename().string();
            if (filename.ends_with(".ckpt") || filename.ends_with(".idx"))
                continue;

            /// More error handling for non stream-shard dir
            auto stream_shard = nlog::Log::streamShardFrom(filename);
            if (stream_shard.hasError())
            {
                LOG_ERROR(logger, "Invalid log filename={}, skip loading it", stream_shard_dir_entry.path().c_str());
                continue;
            }

            /// Make a copy of it since the following pool schedule needs a copy
            auto stream_shard_dir(stream_shard_dir_entry.path());

            /// Config overrides
            auto log_config{default_log_config};
            if (auto iter = log_config_overrides.find(stream_shard.result.stream()); iter != log_config_overrides.end())
            {
                /// File system dir name doesn't contain stream name
                /// update it here
                stream_shard.result.ns = iter->first.ns;
                stream_shard.result.name = iter->first.name;
                log_config = iter->second.config;
            }
            else
            {
                /// This is a left over log since it doesn't exist in \log_config_overrides map
                /// which contains all log configs
                LOG_WARNING(
                    logger, "Ignore log '{}' because it was not found in metadb. Scheduling for deletion.", stream_shard.result.string());

                adhoc_scheduler.trySchedule([log_path = stream_shard_dir_entry.path(), logger_ = logger]() {
                    LOG_INFO(logger_, "Cleaning up left over log {}", log_path.c_str());
                    std::error_code ec;
                    fs::remove_all(log_path, ec);
                });
                continue;
            }

            total_logs += 1;

            pool->scheduleOrThrow([=, this]() {
                setThreadName("LogLoader");
                LOG_INFO(logger, "Loading log in {} in namespace {}", stream_shard_dir.c_str(), ns);

                Stopwatch stopwatch;

                LogPtr log;
                try
                {
                    log = loadLog(stream_shard.result, stream_shard_dir, log_config);
                }
                catch (...)
                {
                    DB::tryLogCurrentException(logger, fmt::format("Error while loading log dir {}", stream_shard_dir.c_str()));
                    return;
                }

                LOG_INFO(
                    logger,
                    "Completed load log in {} in namespace {} with {} segments in {}ms",
                    stream_shard_dir.c_str(),
                    ns,
                    log->numberOfSegments(),
                    stopwatch.elapsedMilliseconds());
            });
        }
    }
    return pool;
}

LogPtr LogManager::loadLog(const StreamShard & stream_shard, const fs::path & log_dir, LogConfigPtr default_config_)
{
    auto log = Log::create(stream_shard, log_dir, default_config_, had_clean_shutdown_prev, scheduler, adhoc_scheduler);

    if (log_dir.extension().c_str() == Log::DELETE_DIR_SUFFIX())
        addLogToBeDeleted(log);
    else if (!current_logs.tryEmplace(stream_shard.id_shard, log))
        throw DB::Exception(
            DB::ErrorCodes::INVALID_STATE,
            "Duplicate log directory for {} are found in {}. It is likely because log directory failure happened while broker was "
            "replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two "
            "directories for this shard. It is recommended to delete the shard in the log directory that is known to have failed "
            "recently",
            stream_shard.string(),
            log_dir.c_str());

    return log;
}

LogConfigPtr LogManager::mergeDefaultConfig(LogConfigPtr log_config) const
{
    if (log_config)
        return log_config;

    return default_log_config->clone();
}

LogPtr LogManager::getOrCreateLog(const StreamShard & stream_shard, LogConfigPtr log_config)
{
    auto log = getLog(stream_shard.id_shard);
    if (log)
    {
        LOG_INFO(
            logger,
            "Found existing log for stream_shard={}, log_ptr={}, next_sn={}",
            stream_shard.string(),
            static_cast<const void *>(log.get()),
            log->nextLogSequence());
        return log;
    }

    /// Create the log if it has not already been created in another thread
    fs::path log_dir = nextLogDir();

    std::string log_dir_name = Log::logDirName(stream_shard);

    log_dir = createLogDirectory(log_dir, stream_shard.ns, log_dir_name);

    log = Log::create(
        stream_shard,
        log_dir,
        mergeDefaultConfig(log_config),
        /*has_clean_shutdown=*/true,
        scheduler,
        adhoc_scheduler);

    [[maybe_unused]] bool result = current_logs.tryEmplace(stream_shard.id_shard, log);
    assert(result);

    LOG_INFO(
        logger,
        "Created new log for shard={} in {} with properties={}, log_ptr={}, next_sn={}",
        stream_shard.string(),
        log_dir.c_str(),
        config->string(),
        static_cast<const void *>(log.get()),
        log->nextLogSequence());

    return log;
}

/// Get the log if it exists, otherwise return nullptr
LogPtr LogManager::getLog(const StreamIDShard & stream_shard) const
{
    LogPtr res;
    current_logs.at(stream_shard, res);
    return res;
}

void LogManager::trim(const std::vector<StreamShardSequence> & shard_sns)
{
    std::unordered_map<fs::path, const std::string *> trimmed;

    for (const auto & tso : shard_sns)
    {
        LogPtr log = getLog(tso.stream_shard.id_shard);
        if (!log)
            continue;

        try
        {
            if (log->trim(tso.sn))
                trimmed.emplace(log->rootDir(), &tso.stream_shard.ns);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(logger, "Failed to trim log {} to sn={} error={}", tso.stream_shard.string(), tso.sn, e.message());
        }
        catch (const std::exception & se)
        {
            LOG_ERROR(logger, "Failed to trim log {} to sn={} error={}", tso.stream_shard.string(), tso.sn, se.what());
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Failed to trim log {} to sn={}", tso.stream_shard.string(), tso.sn));
        }
    }
}

/// Create and check validity of the given directories that are not in the given offline directories, specifically:
/// - Ensure that there are no duplicates in the directory list
/// - Create each directory if it doesn't exist
/// - Check that each path is a readable directory
void LogManager::createAndValidateLogDirs()
{
    std::unordered_set<fs::path> canonical_paths;

    for (const auto & dir : root_dirs)
    {
        try
        {
            if (!fs::exists(dir))
            {
                LOG_INFO(logger, "Log directory {} not found, creating it", dir.c_str());

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
            throw;
        }
    }
}

/// Calculate the number of shards in each directory and find the data directory with fewest shards
const fs::path & LogManager::nextLogDir() const
{
    /// Fast path
    if (root_dirs.size() == 1)
        return root_dirs.front();

    /// Count the number of logs in each parent directory including 0 for empty directories
    std::unordered_map<fs::path, size_t> counts;

    current_logs.apply([&counts](const auto & kv) { ++counts[kv.second->rootDir()]; });

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

    return *min_path;
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

fs::path LogManager::createLogDirectory(const fs::path & log_dir, const std::string & ns, const std::string & log_dir_name)
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

std::vector<LogPtr>
LogManager::getLogs(const fs::path & root_dir, const std::string & ns, std::function<bool(const LogPtr &)> predicate) const
{
    assert(!root_dir.empty());

    /// Step 1: filter according to namespace
    std::vector<LogPtr> logs;

    if (ns.empty())
    {
        logs = current_logs.values();
    }
    else
    {
        current_logs.apply([&](const auto & kv) {
            if (kv.second->streamShard().ns == ns)
                logs.push_back(kv.second);
        });
    }

    std::vector<LogPtr> results;

    /// Step 2: filter according to root_dir and predicate
    for (const auto & log : logs)
    {
        if (log->rootDir() != root_dir)
            continue;

        if (predicate && !predicate(log))
            continue;

        results.push_back(log);
    }

    return results;
}

/// Delete any eligible logs. Only consider logs that are not compacted.
void LogManager::cleanupLogs()
{
    LOG_DEBUG(logger, "Beginning log cleanup...");

    size_t total = 0;
    auto all_logs = current_logs.values();

    try
    {
        for (auto & log : all_logs)
            total += log->deleteOldSegments(log->appliedSequence());
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, "Fail to retention logs");
    }

    if (total > 0)
        LOG_INFO(logger, "Total {} log segments are scheduled for retention", total);

    if (!stopped.test())
        log_retention_task->scheduleAfter(config->retention_check_ms);
}

/// Flush any log which has exceeded its flush interval and has unwritten messages
void LogManager::flushDirtyLogs()
{
    LOG_DEBUG(logger, "Beginning flush dirty logs...");

    auto all_current_logs{current_logs.values()};

    for (auto & log : all_current_logs)
    {
        const auto & stream_shard = log->streamShard();

        auto log_config = log->config();
        try
        {
            auto elapsed = DB::UTCMilliseconds::now() - log->lastFlushedTime();

            LOG_DEBUG(
                logger,
                "Checking if flush is needed on {}.{}-{}, default flush interval {} ms, last flushed: UTC timestamp {}, {} ms passed since "
                "last flush",
                stream_shard.ns,
                stream_shard.name,
                stream_shard.shard(),
                log_config->flush_interval_ms,
                log->lastFlushedTime(),
                elapsed);

            if (elapsed >= log_config->flush_interval_ms)
                log->flush();
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Failed to flush stream shard {}", stream_shard.string()));
        }
    }

    /// if (!stopped.test())
    ///    log_flush_task->scheduleAfter(config->flush_recovery_sn_checkpoint_ms);
}

/// Delete logs marked for deletion. Delete all logs for which `default_log.file_delete_delay_ms`
/// has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will
/// be considered for deletion in the next iteration of `removeLogs`. The next iteration will be executed
/// after the remaining time for the first log that is not deleted. If there are no more `logs_to_be_deleted`,
/// `removeLogs` will be executed after `default_log.file_delete_delay_ms`.
void LogManager::removeLogs()
{
    LOG_DEBUG(logger, "Beginning delete logs...");

    auto file_delete_delay = default_log_config->file_delete_delay_ms;
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
        {
            return file_delete_delay;
        }
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

void LogManager::remove(const std::vector<StreamShard> & stream_shards)
{
    for (const auto & stream_shard : stream_shards)
    {
        try
        {
            if (auto log = getLog(stream_shard.id_shard); log)
                remove(stream_shard);
            else
                LOG_WARNING(logger, "Shard={} which is going to be removed is not found", stream_shard.string());
        }
        catch (...)
        {
            /// FIXME, accept an exception handler in `remove` ?
            DB::tryLogCurrentException(logger, "Exception happened while remove stream shards");
            throw;
        }
    }
}

LogPtr LogManager::remove(const StreamShard & stream_shard)
{
    LogPtr log;
    if (current_logs.at(stream_shard.id_shard, log))
        current_logs.erase(stream_shard.id_shard);

    if (log)
    {
        log->renameDir(Log::logDeleteDirName(stream_shard));

        addLogToBeDeleted(log);

        LOG_INFO(logger, "Log for shard={} is renamed to {} and it scheduled for deletion", stream_shard.string(), log->logDir().c_str());
    }
    else
    {
        LOG_ERROR(logger, "Failed to remove log for {} because it doesn't exist", stream_shard.string());
    }
    return log;
}

void LogManager::updateConfig(
    const std::vector<StreamShard> & stream_shards,
    const std::unordered_map<std::string, int32_t> & flush_settings,
    const std::unordered_map<std::string, int64_t> & retention_settings)
{
    for (const auto & stream_shard : stream_shards)
    {
        if (auto log = getLog(stream_shard.id_shard); log)
            log->updateConfig(flush_settings, retention_settings);
        else
            LOG_WARNING(logger, "Shard={} is not found", stream_shard.string());
    }
}

/// Get the log config if log exists, otherwise return nullptr
LogConfigPtr LogManager::getLogConfig(const StreamIDShard & stream_shard) const
{
    auto log = getLog(stream_shard);
    return log != nullptr ? log->config() : nullptr;
}


void LogManager::writeCleanShutdownFile(const fs::path & dir) const
{
    auto clean_shutdown_file = dir / CLEAN_SHUTDOWN_FILE;
    ::open(clean_shutdown_file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
}

/// \return true if there is clean shutdown file and the file is cleaned and false if the file doesn't exist
bool LogManager::removeCleanShutdownFile(const fs::path & dir) const
{
    auto clean_shutdown_file = dir / CLEAN_SHUTDOWN_FILE;
    if (fs::exists(clean_shutdown_file))
    {
        LOG_INFO(logger, "Skipping recovery for all logs in {} since clean shutdown file was found", dir.c_str());

        /// Delete the clean shutdown file, so that if the broker crashes while loading the log,
        /// it is considered a hard shutdown during next boost up.
        std::error_code ec;
        fs::remove(clean_shutdown_file, ec);
        return true;
    }
    else
    {
        LOG_INFO(logger, "Attempting recovery for all logs in {} since no clean shutdown file was found", dir.c_str());
        return false;
    }
}
}
