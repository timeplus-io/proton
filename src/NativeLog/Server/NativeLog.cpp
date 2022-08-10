#include "NativeLog.h"

#include <NativeLog/Cache/TailCache.h>
#include <NativeLog/Common/LogAppendDescription.h>
#include <NativeLog/Common/LogDirFailureChannel.h>
#include <NativeLog/Log/LogManager.h>
#include <NativeLog/MetaStore/MetaStore.h>
#include <base/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int OK;
}
}

namespace CurrentMetrics
{
extern const Metric BackgroundSchedulePoolNativeLogTask;
}

namespace nlog
{
namespace
{
    const std::string NATIVE_LOG_KEY = "cluster_settings.logstore";
    const std::string NATIVE_LOG_KEY_PREFIX = NATIVE_LOG_KEY + ".";

    AppendResponse logAppendInfoToAppendResponse(const AppendRequest & request, const LogAppendDescription & log_append_info)
    {
        AppendResponse response(request.stream_shard, request.api_version);
        response.sn = log_append_info.seq_metadata.record_sn;
        response.log_start_sn = log_append_info.log_start_sn;
        response.max_timestamp = log_append_info.max_event_timestamp;
        response.append_timestamp = log_append_info.append_timestamp;
        response.error_code = log_append_info.error_code;
        response.error_message = log_append_info.error_message;

        return response;
    }

    DB::CompressionMethodByte codec(const std::string & method)
    {
        if (method.empty() || method == "NONE")
            return DB::CompressionMethodByte::NONE;
        else if (method == "LZ4")
            return DB::CompressionMethodByte::LZ4;
        else if (method == "ZSTD")
            return DB::CompressionMethodByte::ZSTD;
        else if (method == "Multiple")
            return DB::CompressionMethodByte::Multiple;
        else if (method == "Delta")
            return DB::CompressionMethodByte::Delta;
        else if (method == "T64")
            return DB::CompressionMethodByte::T64;
        else if (method == "DoubleDelta")
            return DB::CompressionMethodByte::DoubleDelta;
        else if (method == "Gorilla")
            return DB::CompressionMethodByte::Gorilla;
        else if (method == "AES_128_GCM_SIV")
            return DB::CompressionMethodByte::AES_128_GCM_SIV;
        else if (method == "AES_256_GCM_SIV")
            return DB::CompressionMethodByte::AES_256_GCM_SIV;
        else
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "{} compression method is not supported", method);
    }
}

NativeLog & NativeLog::instance(const DB::ContextPtr & global_context)
{
    static NativeLog native_log(global_context);
    return native_log;
}

NativeLog::NativeLog(DB::ContextPtr global_context_) : global_context(std::move(global_context_)), logger(&Poco::Logger::get("NativeLog"))
{
    assert(global_context);
    const auto & all_configs = global_context->getConfigRef();

    Poco::Util::AbstractConfiguration::Keys native_log_keys;
    all_configs.keys(NATIVE_LOG_KEY, native_log_keys);

    for (const auto & key : native_log_keys)
    {
        if (key != "nativelog")
            continue;

        if (init(key))
            break;
    }
}

bool NativeLog::init(const std::string & key)
{
    const auto & all_configs = global_context->getConfigRef();

    bool enabled = false;
    std::string method;
    std::vector<std::string> paths;

    std::vector<std::tuple<std::string, std::string, void *>> settings
        = {{".enabled", "bool", &enabled},
           {".check_crcs", "bool", &config.check_crcs},
           {".max_schedule_threads", "int32", &config.max_schedule_threads},
           {".max_adhoc_schedule_threads", "int32", &config.max_adhoc_schedule_threads},
           {".metastore_data_dir", "string", &config.meta_dir},
           {".cache_max_cached_entries", "int64", &config.cache_config.max_cached_entries},
           {".cache_max_cached_bytes", "int64", &config.cache_config.max_cached_bytes},
           {".cache_max_cached_entries_per_shard", "int64", &config.cache_config.max_cached_entries_per_shard},
           {".cache_max_cached_bytes_per_shard", "int64", &config.cache_config.max_cached_bytes_per_shard},
           {".log_max_record_size", "int64", &config.log_config.max_record_size},
           {".log_data_dirs", "array(string)", &paths},
           {".log_max_record_size", "int64", &config.log_config.max_record_size},
           {".log_segment_size", "int64", &config.log_config.segment_size},
           {".log_retention_size", "int64", &config.log_config.retention_size},
           {".log_retention_ms", "int64", &config.log_config.retention_ms},
           {".index_interval_bytes", "int64", &config.log_config.index_interval_bytes},
           {".index_interval_records", "int64", &config.log_config.index_interval_records},
           {".index_compression_codec", "string", &method},
           {".fetch_max_wait_ms", "int64", &config.fetch_config.max_wait_ms},
           {".fetch_max_bytes", "int64", &config.fetch_config.max_bytes}};

    auto parse_array_string = [&all_configs](const std::string & k, std::vector<std::string> & str_array) {
        Poco::Util::AbstractConfiguration::Keys arr_elements;
        all_configs.keys(k, arr_elements);

        for (const auto & arr_element : arr_elements)
            str_array.push_back(all_configs.getString(fmt::format("{}.{}", k, arr_element)));
    };

    for (const auto & t : settings)
    {
        auto k = fmt::format("{}{}{}", NATIVE_LOG_KEY_PREFIX, key, std::get<0>(t));
        if (all_configs.has(k))
        {
            const auto & type = std::get<1>(t);
            if (type == "string")
                *static_cast<std::string *>(std::get<2>(t)) = all_configs.getString(k);
            else if (type == "int32")
                *static_cast<int32_t *>(std::get<2>(t)) = all_configs.getInt(k);
            else if (type == "int64")
                *static_cast<int64_t *>(std::get<2>(t)) = all_configs.getInt64(k);
            else if (type == "bool")
                *static_cast<bool *>(std::get<2>(t)) = all_configs.getBool(k);
            else if (type == "array(string)")
                parse_array_string(k, *static_cast<std::vector<std::string> *>(std::get<2>(t)));
        }
    }

    if (!enabled)
    {
        LOG_INFO(logger, "NativeLog config section={}{} is disabled, ignore it", NATIVE_LOG_KEY_PREFIX, key);
        return false;
    }

    for (auto & p : paths)
    {
        fs::path filepath(p);
        fs::create_directories(filepath);
        config.log_dirs.push_back(fs::canonical(filepath));
    }

    config.log_config.codec = codec(method);

    LOG_INFO(logger, "NativeLog init with configs: {}", config.string());

    scheduler = std::make_shared<DB::NLOG::BackgroundSchedulePool>(
        config.max_schedule_threads, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "NLogSched");

    adhoc_scheduler = std::make_shared<ThreadPool>(config.max_adhoc_schedule_threads);

    return true;
}

NativeLog::NativeLog(
    const NativeLogConfig & config_,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
    std::shared_ptr<ThreadPool> adhoc_scheduler_)
    : config(config_)
    , scheduler(std::move(scheduler_))
    , adhoc_scheduler(std::move(adhoc_scheduler_))
    , logger(&Poco::Logger::get("NativeLogServer"))
{
    if (!scheduler)
        scheduler = std::make_shared<DB::NLOG::BackgroundSchedulePool>(
            config.max_schedule_threads, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "NLogSched");

    if (!adhoc_scheduler_)
        adhoc_scheduler = std::make_shared<ThreadPool>(config.max_adhoc_schedule_threads);
}

NativeLog::~NativeLog()
{
    shutdown();
    LOG_INFO(logger, "dtored");
}

void NativeLog::startup()
{
    if (!scheduler)
        return;

    if (inited.test_and_set())
    {
        LOG_ERROR(logger, "Already started");
        return;
    }

    LOG_INFO(logger, "Starting");

    /// Init tail cache
    cache = std::make_shared<TailCache>(config.cache_config);

    /// Init meta store
    auto mstore = std::make_unique<MetaStore>(config.metaStoreConfig());
    meta_store.swap(mstore);
    meta_store->startup();

    /// Init log manager
    std::vector<fs::path> initial_offline_dirs;
    auto lmgr = std::make_unique<LogManager>(
        config.log_dirs,
        initial_offline_dirs,
        config.logConfig(),
        config.logCompactorConfig(),
        config.logManagerConfig(),
        *meta_store,
        scheduler,
        adhoc_scheduler,
        cache);

    log_manager.swap(lmgr);
    log_manager->startup();

    LOG_INFO(logger, "Started");
}

void NativeLog::shutdown()
{
    if (stopped.test_and_set())
        return;

    LOG_INFO(logger, "Stopping");

    if (meta_store)
        meta_store->shutdown();

    if (log_manager)
        log_manager->shutdown();

    LOG_INFO(logger, "Stopped");
}

CreateStreamResponse NativeLog::createStream(const std::string & ns, const CreateStreamRequest & request)
{
    /// FIXME, race, half-way success

    /// First commit the metadata into kvstore
    auto response = meta_store->createStream(ns, request);

    /// Secondly, create shards on file system

    for (int32_t shard = 0; shard < request.shards; ++shard)
    {
        auto log = log_manager->getOrCreateLog(ns, StreamShard{request.stream, response.id, shard}, true, false);
        assert(log);
        (void)log;
    }
    return response;
}

DeleteStreamResponse NativeLog::deleteStream(const std::string & ns, const DeleteStreamRequest & request)
{
    auto list_response{meta_store->listStreams(ns, ListStreamsRequest{request.stream})};
    if (list_response.hasError())
    {
        DeleteStreamResponse resp(0);
        resp.error_code = list_response.error_code;
        resp.error_message.swap(list_response.error_message);
        return resp;
    }

    if (list_response.streams.empty())
    {
        DeleteStreamResponse resp(0);
        resp.error_code = DB::ErrorCodes::RESOURCE_NOT_FOUND;
        resp.error_message = "Not Found";
        return resp;
    }

    /// FIXME, race, half-way success
    auto resp{meta_store->deleteStream(ns, request)};
    if (resp.hasError())
        return resp;

    std::unordered_map<std::string, std::vector<StreamShard>> streams_to_deleted;
    for (const auto & stream_desc : list_response.streams)
        for (int32_t i = 0; i < stream_desc.shards; ++i)
            streams_to_deleted[stream_desc.ns].emplace_back(stream_desc.stream, stream_desc.id, i);

    for (const auto & ns_stream_shards : streams_to_deleted)
        log_manager->remove(ns_stream_shards.first, ns_stream_shards.second);

    return resp;
}

ListStreamsResponse NativeLog::listStreams(const std::string & ns, const ListStreamsRequest & request)
{
    return meta_store->listStreams(ns, request);
}

RenameStreamResponse NativeLog::renameStream(const std::string & ns, const RenameStreamRequest & request)
{
    return meta_store->renameStream(ns, request);
}

AppendResponse NativeLog::append(const std::string & ns, AppendRequest & request)
{
    auto log = log_manager->getLog(ns, request.stream_shard);
    if (!log)
        throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Stream {} doesnt exist", request.stream_shard.string());

    auto log_append_info{log->append(request.record)};

    return logAppendInfoToAppendResponse(request, log_append_info);
}

FetchResponse NativeLog::fetch(const std::string & ns, const FetchRequest & request)
{
    std::vector<FetchResponse::FetchedData> fetched_data;
    fetched_data.reserve(request.fetch_descs.size());

    for (const auto & fetch_desc : request.fetch_descs)
    {
        auto log = log_manager->getLog(ns, fetch_desc.stream_shard);
        if (!log)
        {
            LOG_ERROR(logger, "Stream {} doesn't exist", fetch_desc.stream_shard.string());
            fetched_data.push_back({fetch_desc.stream_shard, DB::ErrorCodes::RESOURCE_NOT_FOUND, {}});
            continue;
        }

        fetched_data.push_back(
            {fetch_desc.stream_shard, DB::ErrorCodes::OK, log->fetch(fetch_desc.sn, fetch_desc.max_size, fetch_desc.max_wait_ms, fetch_desc.position)});
    }

    return FetchResponse(std::move(fetched_data));
}

TruncateResponse NativeLog::truncate(const std::string & ns, const TruncateRequest & request)
{
    (void)ns;
    (void)request;
    return TruncateResponse{0};
}

TranslateTimestampsResponse NativeLog::translateTimestamps(const std::string & ns, const TranslateTimestampsRequest & request)
{
    std::vector<int64_t> timestamps;
    timestamps.reserve(request.shards.size());

    TranslateTimestampsResponse response(request.stream);
    for (auto shard: request.shards)
    {
        auto log = log_manager->getLog(ns, StreamShard{request.stream, shard});
        if (!log)
        {
            LOG_ERROR(logger, "Stream {} doesn't exist", request.stream.name);
            response.error_code = DB::ErrorCodes::RESOURCE_NOT_FOUND;
            response.sequences.push_back(EARLIEST_SN);
        }
        else
            response.sequences.push_back(log->sequenceForTimestamp(request.timestamp, request.append_time));
    }

    return response;
}

std::optional<std::pair<uint64_t, std::vector<String>>> NativeLog::getLocalStreamInfo(const StreamDescription & desc) const noexcept
{
    /// Size in metastore skip, the value is small and difficult to get

    /// Log size, for now all shards in one node.
    /// TODO: Skip remote shards.
    uint64_t bytes_size = 0;
    std::vector<std::string> paths;
    paths.reserve(desc.shards);
    for (int32_t shard = 0; shard < desc.shards; ++shard)
    {
        StreamShard stream_shard{desc.stream, desc.id, shard};
        auto log = log_manager->getLog(desc.ns, stream_shard);
        if (!log)
            return {}; /// failure.

        bytes_size += log->size();
        paths.push_back(log->logDir());
    }
    return std::pair{bytes_size, paths};
}

}
