#pragma once

#include "NativeLogConfig.h"

#include <NativeLog/Requests/AppendRequest.h>
#include <NativeLog/Requests/AppendResponse.h>
#include <NativeLog/Requests/CreateStreamRequest.h>
#include <NativeLog/Requests/CreateStreamResponse.h>
#include <NativeLog/Requests/DeleteStreamRequest.h>
#include <NativeLog/Requests/DeleteStreamResponse.h>
#include <NativeLog/Requests/FetchRequest.h>
#include <NativeLog/Requests/FetchResponse.h>
#include <NativeLog/Requests/ListStreamsRequest.h>
#include <NativeLog/Requests/ListStreamsResponse.h>
#include <NativeLog/Requests/RenameStreamRequest.h>
#include <NativeLog/Requests/RenameStreamResponse.h>
#include <NativeLog/Requests/TranslateTimestampsRequest.h>
#include <NativeLog/Requests/TranslateTimestampsResponse.h>
#include <NativeLog/Requests/TruncateRequest.h>
#include <NativeLog/Requests/TruncateResponse.h>
#include <NativeLog/Requests/UpdateStreamRequest.h>
#include <NativeLog/Requests/UpdateStreamResponse.h>

#include <Interpreters/Context_fwd.h>
#include <Common/BackgroundSchedulePool.h>

#include <boost/noncopyable.hpp>

namespace nlog
{
class MetaStore;
class LogManager;
class TailCache;

/// NativeLog orchestrates different major components of it
class NativeLog final : private boost::noncopyable
{
public:
    static NativeLog & instance(const DB::ContextPtr & global_context);

    explicit NativeLog(
        const NativeLogConfig & config_,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_ = nullptr,
        std::shared_ptr<ThreadPool> adhoc_scheduler_ = nullptr);

    explicit NativeLog(DB::ContextPtr global_context);
    ~NativeLog();

    void startup();
    void shutdown();

    /// Admin APIs
    CreateStreamResponse createStream(const std::string & ns, const CreateStreamRequest & request);
    UpdateStreamResponse updateStream(const std::string & ns, const UpdateStreamRequest & request);
    DeleteStreamResponse deleteStream(const std::string & ns, const DeleteStreamRequest & request);
    ListStreamsResponse listStreams(const std::string & ns, const ListStreamsRequest & request);
    RenameStreamResponse renameStream(const std::string & ns, const RenameStreamRequest & request);

    /// Data APIs
    AppendResponse append(const std::string & ns, AppendRequest & request);
    FetchResponse fetch(const std::string & ns, const FetchRequest & request);
    TruncateResponse truncate(const std::string & ns, const TruncateRequest & request);
    TranslateTimestampsResponse translateTimestamps(const std::string & ns, const TranslateTimestampsRequest & request);

    bool enabled() const { return scheduler != nullptr; }

    TailCache & getCache() const { return *cache; }

    /// nullopt -> Not found
    std::optional<std::pair<uint64_t, std::vector<std::string>>> getLocalStreamInfo(const StreamDescription & desc) const noexcept;

private:
    bool init(const std::string & key);

private:
    DB::ContextPtr global_context;

    std::atomic_flag inited;
    std::atomic_flag stopped;

    NativeLogConfig config;

    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;
    std::shared_ptr<ThreadPool> adhoc_scheduler;

    std::shared_ptr<TailCache> cache;
    std::unique_ptr<MetaStore> meta_store;
    std::unique_ptr<LogManager> log_manager;

    Poco::Logger * logger;
};
}
