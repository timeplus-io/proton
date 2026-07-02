#include <Cluster/LocalLog/Log/LogManager.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>

#include <Common/CurrentMetrics.h>
#include <Common/LogstoreRetentionSettings.h>
#include <Common/logger_useful.h>

#include <limits>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB::ErrorCodes
{
extern const int CANNOT_LOAD_CONFIG;
}

namespace cluster
{

namespace
{
nlog::LogConfigMap logConfigs(meta::MetaStore & meta_store, const StoreConfig & config)
{
    /// Fetch all stream configs from metastore to init data store
    auto list_stream_resp = meta_store.listStreams();
    if (list_stream_resp->hasError())
    {
        const auto & err = list_stream_resp->error();
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_LOAD_CONFIG,
            "Failed to load all stream configurations error_code={} error_message={}",
            err.error_code,
            err.error_message);
    }

    nlog::LogConfigMap log_configs;

    for (auto & stream_desc : list_stream_resp->data().stream_descs)
    {
        auto new_config = config.log_config->clone();
        if (stream_desc->flush_bytes > 0)
            new_config->flush_bytes = stream_desc->flush_bytes;

        if (stream_desc->flush_messages > 0)
            new_config->flush_interval_entries = stream_desc->flush_messages;

        if (stream_desc->flush_ms > 0)
            new_config->flush_interval_ms = stream_desc->flush_ms;

        if (stream_desc->retention_bytes == DB::kLogstoreRetentionNoLimit)
            new_config->retention_size = 0;
        else if (stream_desc->retention_bytes > 0)
            new_config->retention_size = stream_desc->retention_bytes;

        if (stream_desc->retention_ms == DB::kLogstoreRetentionNoLimit)
            new_config->retention_ms = 0;
        else if (stream_desc->retention_ms > 0)
        {
            constexpr auto int64_max_as_u64 = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
            new_config->retention_ms = (stream_desc->retention_ms > int64_max_as_u64) ? std::numeric_limits<int64_t>::max()
                                                                                      : static_cast<int64_t>(stream_desc->retention_ms);
        }

        /// FIXME, this override is not necessary good
        new_config->codec = stream_desc->codec;

        log_configs.emplace(std::move(stream_desc->stream), nlog::LogConfigEntry{std::move(new_config)});
    }

    return log_configs;
}
}

NativeLog::NativeLog(
    StoreConfigPtr config_, meta::MetaStore & meta_store_, DB::NLOG::BackgroundSchedulePool & scheduler_, ThreadPool & adhoc_scheduler_)
    : config(std::move(config_))
    , meta_store(meta_store_)
    , scheduler(scheduler_)
    , adhoc_scheduler(adhoc_scheduler_)
    , logger(getLogger("NativeLog"))
{
}

void NativeLog::init()
{
    initLogManager();
}

NativeLog::~NativeLog() noexcept
{
    shutdown();
}

void NativeLog::startup()
{
    if (started.test_and_set())
        return;

    LOG_INFO(logger, "Starting");

    /// We will need defer init to startup,
    /// because NativeLog depends on MetaStore's startup to init
    init();

    if (log_manager)
        log_manager->startup();

    ready = true;
    LOG_INFO(logger, "Started");
}

void NativeLog::shutdown()
{
    if (stopped.test_and_set())
        return;

    /// Fence off all requests
    ready = false;

    LOG_INFO(logger, "Stopping");

    if (log_manager)
        log_manager->shutdown();

    LOG_INFO(logger, "Stopped");
}

void NativeLog::initLogManager()
{
    auto log_mgr_config = std::make_shared<nlog::LogManagerConfig>();
    log_mgr_config->retention_check_ms = config->retention_check_ms;

    log_manager = std::make_unique<nlog::LogManager>(
        config->data_dirs, std::move(log_mgr_config), config->log_config, logConfigs(meta_store, *config), scheduler, adhoc_scheduler);
}

void NativeLog::shutdown(const StreamIDShard & stream_shard, bool /*hot_close*/)
{
    if (auto log = log_manager->getLog(stream_shard); log)
        log->shutdown();
    else
        LOG_WARNING(logger, "Can't shutdown Log for stream={{{}}}. Not found", stream_shard.string());
}

void NativeLog::restart(const StreamIDShard & stream_shard)
{
    if (auto log = log_manager->getLog(stream_shard); log)
        log->restart();
    else
        LOG_WARNING(logger, "Can't restart Log for stream={{{}}}. Not found", stream_shard.string());
}
}
