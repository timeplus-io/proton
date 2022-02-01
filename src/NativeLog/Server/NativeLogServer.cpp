#include "NativeLogServer.h"

#include <NativeLog/Common/LogAppendInfo.h>
#include <NativeLog/Common/LogDirFailureChannel.h>

#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
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
ProduceResponse logAppendInfoToProduceResponse(LogAppendInfo log_append_info)
{
    ProduceResponse response;
    response.first_offset = log_append_info.first_offset.message_offset;
    response.last_offset = log_append_info.last_offset;
    response.max_timestamp = log_append_info.max_timestamp;
    response.append_timestamp = log_append_info.append_timestamp;
    response.log_start_offset = log_append_info.log_start_offset;
    response.error.error_message = log_append_info.error_message;

    return response;
}
}

NativeLogServer::NativeLogServer(
    const NativeLogConfig & config_,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
    std::shared_ptr<ThreadPool> adhoc_scheduler_)
    : config(config_), scheduler(std::move(scheduler_)), adhoc_scheduler(std::move(adhoc_scheduler_)), logger(&Poco::Logger::get("NativeLogServer"))
{
    if (!scheduler)
        scheduler = std::make_shared<DB::NLOG::BackgroundSchedulePool>(
            config.max_schedule_threads, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "NlogSched");

    if (!adhoc_scheduler_)
        adhoc_scheduler = std::make_shared<ThreadPool>(config.max_adhoc_schedule_threads);
}

void NativeLogServer::startup()
{
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
        scheduler,
        adhoc_scheduler);

    log_manager.swap(lmgr);
    auto topic_response = meta_store->listTopics("", {});
    std::unordered_map<std::string, std::vector<Topic>> topics;

    for (auto & topic_info : topic_response.topics)
        topics[topic_info.ns].emplace_back(topic_info.name, topic_info.id);

    log_manager->startup(topics);
}

CreateTopicResponse NativeLogServer::createTopic(const std::string & ns, const CreateTopicRequest & request)
{
    /// FIXME, race, half-way success

    /// First commit the metadata into kvstore
    auto response = meta_store->createTopic(ns, request);
    /// Secondly, create partitions on file system

    for (uint32_t partition = 0; partition < request.partitions; ++partition)
    {
        auto log = log_manager->getOrCreateLog(ns, {request.name, partition}, true, false, response.topic_info.id);
        assert(log);
        (void)log;
    }
    return response;
}

DeleteTopicResponse NativeLogServer::deleteTopic(const std::string & ns, const DeleteTopicRequest & request)
{
    /// FIXME, race, half-way success
    auto topic_response{meta_store->listTopics(ns, {request.name})};
    if (topic_response.topics.empty())
    {
        DeleteTopicResponse resp;
        resp.error.error_message = "NotFound";
        return resp;
    }

    auto resp{meta_store->deleteTopic(ns, request)};

    std::unordered_map<std::string, std::vector<TopicShard>> topics_to_deleted;
    for (const auto & topic : topic_response.topics)
        for (int32_t i = 0; i < topic.partitions; ++i)
            topics_to_deleted[topic.ns].emplace_back(topic.name, i);

    for (const auto & ns_topic_shards : topics_to_deleted)
        log_manager->remove(ns_topic_shards.first, ns_topic_shards.second);

    return resp;
}

ListTopicsResponse NativeLogServer::listTopics(const std::string & ns, const ListTopicsRequest & request)
{
    return meta_store->listTopics(ns, request);
}

ProduceResponse NativeLogServer::produce(const std::string & ns, const ProduceRequest & request)
{
    TopicShard topic_shard{request.topic, request.partition};
    auto log = log_manager->getLog(ns, topic_shard);
    if (!log)
        throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Topic {} doesnt exist", topic_shard.string());

    auto log_append_info{log->append(const_cast<MemoryRecords&>(request.batch))};
    return logAppendInfoToProduceResponse(log_append_info);
}

FetchResponse NativeLogServer::fetch(const std::string & ns, const FetchRequest & request)
{
    FetchResponse response;
    response.data.reserve(request.offsets.size());

    for (const auto & tpo : request.offsets)
    {
        TopicShard topic_shard{tpo.topic, static_cast<uint32_t>(tpo.partition)};
        auto log = log_manager->getLog(ns, topic_shard);
        if (!log)
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Topic {} doesnt exist", topic_shard.string());

       response.data.push_back({tpo.topic, tpo.partition, log->read(tpo.offset, tpo.max_size)});
    }
    return response;
}

TruncateResponse NativeLogServer::truncate(const std::string & ns, const TruncateRequest & request)
{
    (void)ns;
    (void)request;
    return {};
}

}
