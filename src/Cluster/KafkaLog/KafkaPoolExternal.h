#pragma once

#include <Cluster/KafkaLog/KafkaWALSimpleConsumer.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/noncopyable.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}
}

namespace cluster::klog
{
namespace
{
int32_t bucketFetchWaitMax(int32_t fetch_wait_max_ms)
{
    if (fetch_wait_max_ms <= 10)
        return 10;
    else if (fetch_wait_max_ms <= 20)
        return 20;
    else if (fetch_wait_max_ms <= 30)
        return 30;
    else if (fetch_wait_max_ms <= 40)
        return 40;
    else if (fetch_wait_max_ms <= 50)
        return 50;
    else if (fetch_wait_max_ms <= 100)
        return 100;
    else if (fetch_wait_max_ms <= 200)
        return 200;
    else if (fetch_wait_max_ms <= 300)
        return 300;
    else if (fetch_wait_max_ms <= 400)
        return 400;
    else if (fetch_wait_max_ms <= 500)
        return 500;

    return 500;
}
}

class KafkaPoolExternal final : private boost::noncopyable
{
public:
    static KafkaPoolExternal & instance()
    {
        static KafkaPoolExternal kafka_pool_external;
        return kafka_pool_external;
    }

    KafkaPoolExternal() : log(getLogger("KafkaPoolExternal")) { }

    KafkaWALSimpleConsumerPtr getOrCreateStreaming(const String & brokers, const KafkaWALAuth & auth, int32_t fetch_wait_max_ms = 200)
    {
        assert(!brokers.empty());

        fetch_wait_max_ms = bucketFetchWaitMax(fetch_wait_max_ms);

        std::lock_guard lock{external_streaming_lock};

        if (!consumers.contains(brokers))
            /// FIXME, configurable max cached consumers
            consumers.emplace(brokers, std::pair<size_t, KafkaWALSimpleConsumerPtrs>(100, KafkaWALSimpleConsumerPtrs{}));

        /// FIXME, remove cached cluster
        if (consumers.size() > 1000)
            throw DB::Exception(DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Too many external Kafka cluster registered");

        auto & broker_consumers = consumers[brokers];

        for (const auto & consumer : broker_consumers.second)
        {
            const auto & consumer_settings = consumer->getSettings();
            if (consumer.use_count() == 1 && consumer_settings.fetch_wait_max_ms == fetch_wait_max_ms)
            {
                LOG_INFO(log, "Reusing external Kafka consume with settings={}", consumer_settings.string());
                return consumer;
            }
        }

        /// consumer is used up and if we didn't reach maximum
        if (broker_consumers.second.size() < broker_consumers.first)
        {
            if (!boost::iequals(auth.security_protocol, "plaintext") && !boost::iequals(auth.security_protocol, "sasl_ssl"))
                throw DB::Exception(
                    DB::ErrorCodes::NOT_IMPLEMENTED,
                    "Invalid logstore kafka settings security_protocol: {}. Only plaintext or sasl_ssl are supported",
                    auth.security_protocol);

            /// Create one
            auto ksettings = std::make_unique<KafkaWALSettings>();
            ksettings->fetch_wait_max_ms = fetch_wait_max_ms;
            ksettings->brokers = brokers;
            /// Streaming WALs have a different group ID
            ksettings->group_id += "-tp-external-streaming-query-" + std::to_string(broker_consumers.second.size() + 1);
            ksettings->auth = auth;
            /// We don't care offset checkpointing for WALs used for streaming processing,
            /// No auto commit
            ksettings->enable_auto_commit = false;

            LOG_INFO(log, "Create new external Kafka consume with settings={}", ksettings->string());

            auto consumer = std::make_shared<KafkaWALSimpleConsumer>(std::move(ksettings));
            consumer->startup();
            broker_consumers.second.push_back(consumer);
            return consumer;
        }
        else
        {
            LOG_ERROR(log, "External streaming processing pool in cluster={} is used up, size={}", brokers, broker_consumers.first);
            throw DB::Exception(
                DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Max external streaming processing pool size has been reached");
        }
    }

private:
    std::mutex external_streaming_lock;
    std::unordered_map<String, std::pair<size_t, KafkaWALSimpleConsumerPtrs>> consumers;

    LoggerPtr log;
};
}
