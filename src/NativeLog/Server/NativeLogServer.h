#pragma once

#include "NativeLogConfig.h"

#include <NativeLog/Log/LogManager.h>
#include <NativeLog/MetaStore/MetaStore.h>

#include <NativeLog/Requests/CreateTopicRequest.h>
#include <NativeLog/Requests/CreateTopicResponse.h>
#include <NativeLog/Requests/DeleteTopicRequest.h>
#include <NativeLog/Requests/DeleteTopicResponse.h>
#include <NativeLog/Requests/FetchRequest.h>
#include <NativeLog/Requests/FetchResponse.h>
#include <NativeLog/Requests/ListTopicsRequest.h>
#include <NativeLog/Requests/ListTopicsResponse.h>
#include <NativeLog/Requests/ProduceRequest.h>
#include <NativeLog/Requests/ProduceResponse.h>
#include <NativeLog/Requests/TruncateRequest.h>
#include <NativeLog/Requests/TruncateResponse.h>

#include <Common/BackgroundSchedulePool.h>

#include <boost/noncopyable.hpp>

namespace nlog
{
/// NativeLogServer orchestrate different major components of NativeLog
class NativeLogServer final : private boost::noncopyable
{
public:
    explicit NativeLogServer(
        const NativeLogConfig & config_,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_ = nullptr,
        std::shared_ptr<ThreadPool> adhoc_scheduler_ = nullptr);
    void startup();

    /// Admin APIs
    CreateTopicResponse createTopic(const std::string & ns, const CreateTopicRequest & request);
    DeleteTopicResponse deleteTopic(const std::string & ns, const DeleteTopicRequest & request);
    ListTopicsResponse listTopics(const std::string & ns, const ListTopicsRequest & request);

    /// Data APIs
    ProduceResponse produce(const std::string & ns, const ProduceRequest & request);
    FetchResponse fetch(const std::string & ns, const FetchRequest & request);
    TruncateResponse truncate(const std::string & ns, const TruncateRequest & request);

private:
    NativeLogConfig config;

    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;
    std::shared_ptr<ThreadPool> adhoc_scheduler;

    std::unique_ptr<MetaStore> meta_store;
    std::unique_ptr<LogManager> log_manager;

    Poco::Logger * logger;
};
}
