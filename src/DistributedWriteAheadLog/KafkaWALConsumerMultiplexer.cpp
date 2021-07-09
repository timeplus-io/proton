#include "KafkaWALConsumerMultiplexer.h"

#include <Common/setThreadName.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INVALID_OPERATION;
}
}

namespace DWAL
{
KafkaWALConsumerMultiplexer::KafkaWALConsumerMultiplexer(std::unique_ptr<KafkaWALSettings> settings)
    : shared_subscription_flush_threshold_count(settings->shared_subscription_flush_threshold_count)
    , shared_subscription_flush_threshold_bytes(settings->shared_subscription_flush_threshold_bytes)
    , shared_subscription_flush_threshold_ms(settings->shared_subscription_flush_threshold_ms)
    , consumer(std::make_unique<KafkaWALConsumer>(std::move(settings)))
    , poller(1)
    , log(&Poco::Logger::get("KafkaWALConsumerMultiplexer"))
{
}

KafkaWALConsumerMultiplexer::~KafkaWALConsumerMultiplexer()
{
    shutdown();
}

void KafkaWALConsumerMultiplexer::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    consumer->startup();

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });

    LOG_INFO(log, "Started");
}

void KafkaWALConsumerMultiplexer::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    consumer->shutdown();
    poller.wait();
    LOG_INFO(log, "Stopped");
}

KafkaWALConsumerMultiplexer::Result
KafkaWALConsumerMultiplexer::addSubscription(const TopicPartitionOffset & tpo, ConsumeCallback callback, void * data)
{
    assert(callback && consumer);

    std::weak_ptr<CallbackContext> ctx;
    {
        std::lock_guard lock{callbacks_mutex};

        auto iter = callbacks.find(tpo.topic);
        if (iter != callbacks.end())
        {
            /// Found topic, check partition
            auto pos = std::find(iter->second->partitions.begin(), iter->second->partitions.end(), tpo.partition);
            if (pos != iter->second->partitions.end())
            {
                return {DB::ErrorCodes::INVALID_OPERATION, {}};
            }
        }

        auto res = consumer->addSubscriptions({tpo});
        if (res != DB::ErrorCodes::OK)
        {
            return {res, {}};
        }

        if (iter == callbacks.end())
        {
            auto s_ctx = std::make_shared<CallbackContext>(callback, data, tpo.partition);
            ctx = s_ctx;

            callbacks.emplace(tpo.topic, s_ctx);
        }
        else
        {
            /// Found topic, but with new partition.
            /// Override existing callback, data and add new partition
            iter->second->callback = callback;
            iter->second->data = data;
            iter->second->partitions.push_back(tpo.partition);

            ctx = iter->second;
        }
    }

    LOG_INFO(log, "Successfully add subscription to topic={} partition={} offset={}", tpo.topic, tpo.partition, tpo.offset);

    return {DB::ErrorCodes::OK, ctx};
}

int32_t KafkaWALConsumerMultiplexer::removeSubscription(const TopicPartitionOffset & tpo)
{
    assert(consumer);
    {
        std::lock_guard lock{callbacks_mutex};

        auto iter = callbacks.find(tpo.topic);
        if (iter == callbacks.end())
        {
            return DB::ErrorCodes::INVALID_OPERATION;
        }

        auto pos = std::find(iter->second->partitions.begin(), iter->second->partitions.end(), tpo.partition);
        if (pos == iter->second->partitions.end())
        {
            return DB::ErrorCodes::INVALID_OPERATION;
        }

        auto res = consumer->removeSubscriptions({tpo});
        if (res != DB::ErrorCodes::OK)
        {
            return res;
        }

        iter->second->partitions.erase(pos);

        if (iter->second->partitions.empty())
        {
            callbacks.erase(iter);
        }
    }

    LOG_INFO(log, "Successfully remove subscription to topic={} partition={}", tpo.topic, tpo.partition);

    return DB::ErrorCodes::OK;
}

void KafkaWALConsumerMultiplexer::backgroundPoll()
{
    LOG_INFO(log, "Polling consumer multiplexer started");
    setThreadName("KWalCMPoller");

    auto last_flush = DB::MonotonicSeconds::now();
    while (!stopped.test())
    {
        auto result = consumer->consume(shared_subscription_flush_threshold_count, shared_subscription_flush_threshold_ms);

        if (!result.records.empty())
        {
            /// Consume what has been returned regardless the error
            handleResult(std::move(result));
            assert(result.records.empty());
        }

        if (DB::MonotonicSeconds::now() - last_flush >= 10)
        {
            flush();
            last_flush = DB::MonotonicSeconds::now();
        }
    }

    LOG_INFO(log, "Polling consumer multiplexer stopped");
}

void KafkaWALConsumerMultiplexer::flush() const
{
    std::vector<CallbackContextPtr> due_callbacks;
    {
        std::lock_guard lock{callbacks_mutex};

        for (const auto & topic_callback : callbacks)
        {
            if (DB::MonotonicMilliseconds::now() - topic_callback.second->last_call_ts >= shared_subscription_flush_threshold_ms)
            {
                due_callbacks.push_back(topic_callback.second);
            }
        }
    }

    for (const auto & callback_ctx : due_callbacks)
    {
        callback_ctx->callback({}, callback_ctx->data);
        callback_ctx->last_call_ts = DB::MonotonicMilliseconds::now();
    }
}

void KafkaWALConsumerMultiplexer::handleResult(ConsumeResult result) const
{
    /// Categorize results according to topic
    std::unordered_map<std::string, RecordPtrs> all_topic_records;

    for (auto & record : result.records)
    {
        all_topic_records[record->topic].push_back(std::move(record));
    }

    for (auto & topic_records : all_topic_records)
    {
        CallbackContextPtr callback_ctx;
        {
            std::lock_guard lock{callbacks_mutex};

            auto iter = callbacks.find(topic_records.first);
            if (likely(iter != callbacks.end()))
            {
                callback_ctx = iter->second;
            }
        }

        if (likely(callback_ctx))
        {
            callback_ctx->callback(std::move(topic_records.second), callback_ctx->data);
            callback_ctx->last_call_ts = DB::MonotonicMilliseconds::now();
        }
    }
}

int32_t KafkaWALConsumerMultiplexer::commit(const TopicPartitionOffset & tpo)
{
    assert(consumer);
    return consumer->commit({tpo});
}
}
