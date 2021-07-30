#include "KafkaWALConsumer.h"
#include "KafkaWALCommon.h"

#include <Common/Exception.h>
#include <common/ClockUtils.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_NOT_INITED;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int DWAL_FATAL_ERROR;
    extern const int INVALID_OPERATION;
}
}

namespace DWAL
{
KafkaWALConsumer::KafkaWALConsumer(std::unique_ptr<KafkaWALSettings> settings_)
    : settings(std::move(settings_))
    , consumer_handle(nullptr, rd_kafka_destroy)
    , log(&Poco::Logger::get("KafkaWALConsumer"))
    , stats(std::make_unique<KafkaWALStats>(log, "consumer"))
{
}

KafkaWALConsumer::~KafkaWALConsumer()
{
    shutdown();
}

void KafkaWALConsumer::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initHandle();

    LOG_INFO(log, "Started");
}

void KafkaWALConsumer::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    stopConsume();
    LOG_INFO(log, "Stopped");
}

void KafkaWALConsumer::initHandle()
{
    /// 1) use high level Kafka consumer
    /// 2) manually manage offset commit
    /// 3) offsets are stored in brokers and on application side.
    ///     3.1) In normal cases, application side offsets overrides offsets in borkers
    ///     3.2) In corruption cases (application offsets corruption), use offsets in borkers
    std::vector<std::pair<std::string, std::string>> consumer_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("group.id", settings->group_id),
        /// Enable auto offset commit
        std::make_pair("enable.auto.commit", "true"),
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)),
        std::make_pair("fetch.message.max.bytes", std::to_string(settings->fetch_message_max_bytes)),
        std::make_pair("fetch.wait.max.ms", std::to_string(settings->fetch_wait_max_ms)),
        std::make_pair("enable.auto.offset.store", "false"),

        /// By default offset.store.method is broker. Enabling it gives a warning message
        /// https://github.com/edenhill/librdkafka/pull/3035
        /// std::make_pair("offset.store.method", "broker"),
        std::make_pair("enable.partition.eof", "false"),
        std::make_pair("queued.min.messages", std::to_string(settings->queued_min_messages)),
        std::make_pair("queued.max.messages.kbytes", std::to_string(settings->queued_max_messages_kbytes)),
        /// Incremental partition assignment / unassignment
        std::make_pair("partition.assignment.strategy", "cooperative-sticky"),
        /// Consumer group membership heartbeat timeout
        std::make_pair("session.timeout.ms", std::to_string(settings->session_timeout_ms)),
        std::make_pair("max.poll.interval.ms", std::to_string(settings->max_poll_interval_ms)),
        std::make_pair("auto.offset.reset", settings->auto_offset_reset),
        /// ensuring no on-the-wire or on-disk corruption to the messages occurred
        std::make_pair("check.crcs", std::to_string(settings->check_crcs)),
        std::make_pair("statistics.interval.ms", std::to_string(settings->statistic_internal_ms)),
        std::make_pair("security.protocol", settings->security_protocol.c_str()), 
    };

    if (!settings->debug.empty())
    {
        consumer_params.emplace_back("debug", settings->debug);
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_conf_set_stats_cb(kconf, &KafkaWALStats::logStats);
        rd_kafka_conf_set_error_cb(kconf, &KafkaWALStats::logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &KafkaWALStats::logThrottle);

        /// Consumer offset commits
        /// rd_kafka_conf_set_offset_commit_cb(kconf, &KafkaWALStats::logOffsetCommits);
    };

    consumer_handle = initRdKafkaHandle(RD_KAFKA_CONSUMER, consumer_params, stats.get(), cb_setup);

    /// Forward all events from main queue to consumer queue
    /// rd_kafka_poll shall not be invoked after this forwarding. After the fowarding,
    /// invoking rd_kafka_consumer_poll perodically will trigger error_cb, stats_cb, throttle_ct
    /// etc callbacks ?
    /// https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#threads-and-callbacks
    rd_kafka_poll_set_consumer(consumer_handle.get());
}

int32_t KafkaWALConsumer::addSubscriptions(const TopicPartitionOffsets & partitions_)
{
    auto topic_partitions = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>(
        rd_kafka_topic_partition_list_new(partitions_.size()), rd_kafka_topic_partition_list_destroy);

    for (const auto & partition : partitions_)
    {
        auto new_partition = rd_kafka_topic_partition_list_add(topic_partitions.get(), partition.topic.c_str(), partition.partition);
        new_partition->offset = partition.offset;
    }

    auto err = rd_kafka_incremental_assign(consumer_handle.get(), topic_partitions.get());
    if (err)
    {
        LOG_ERROR(log, "Failed to assign partitions incrementally, error={}", rd_kafka_error_string(err));

        auto ret_code = mapErrorCode(rd_kafka_error_code(err), rd_kafka_error_is_retriable(err));
        rd_kafka_error_destroy(err);
        return ret_code;
    }

    return DB::ErrorCodes::OK;
}

int32_t KafkaWALConsumer::removeSubscriptions(const TopicPartitionOffsets & partitions_)
{
    auto topic_partitions = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>(
        rd_kafka_topic_partition_list_new(partitions_.size()), rd_kafka_topic_partition_list_destroy);

    for (const auto & partition : partitions_)
    {
        rd_kafka_topic_partition_list_add(topic_partitions.get(), partition.topic.c_str(), partition.partition);
    }

    auto err = rd_kafka_incremental_unassign(consumer_handle.get(), topic_partitions.get());
    if (err)
    {
        LOG_ERROR(log, "Failed to unassign partitions incrementally, error={}", rd_kafka_error_string(err));

        auto ret_code = mapErrorCode(rd_kafka_error_code(err), rd_kafka_error_is_retriable(err));
        rd_kafka_error_destroy(err);
        return ret_code;
    }

    return DB::ErrorCodes::OK;
}

ConsumeResult KafkaWALConsumer::consume(uint32_t count, int32_t timeout_ms)
{
    ConsumeResult result;
    if (count > 100)
    {
        result.records.reserve(100);
    }
    else
    {
        result.records.reserve(count);
    }

    auto abs_time = DB::MonotonicMilliseconds::now() + timeout_ms;

    for (uint32_t i = 0; i < count; ++i)
    {
        auto rkmessage = rd_kafka_consumer_poll(consumer_handle.get(), timeout_ms);
        if (likely(rkmessage))
        {
            if (likely(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR))
            {
                result.records.push_back(kafkaMsgToRecord(rkmessage, true));
                rd_kafka_message_destroy(rkmessage);
            }
            else
            {
                LOG_ERROR(log, "Failed to consume error={}", rd_kafka_message_errstr(rkmessage));

                result.err = mapErrorCode(rkmessage->err);
                rd_kafka_message_destroy(rkmessage);
                break;
            }
        }

        auto now = DB::MonotonicMilliseconds::now();
        if (now >= abs_time)
        {
            /// Timed up
            break;
        }
        else
        {
            timeout_ms = abs_time - now;
        }
    }
    return result;
}

int32_t KafkaWALConsumer::stopConsume()
{
    if (consume_stopped.test_and_set())
    {
        LOG_ERROR(log, "Already stopped");
        return DB::ErrorCodes::INVALID_OPERATION;
    }

    LOG_INFO(log, "Closing consumer");

    auto err = rd_kafka_consumer_close(consumer_handle.get());
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to stop consuming, error={}", rd_kafka_err2str(err));
        return mapErrorCode(err);
    }

    LOG_INFO(log, "Closed consumer");
    return DB::ErrorCodes::OK;
}

int32_t KafkaWALConsumer::commit(const TopicPartitionOffsets & tpos)
{
    std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *> topic_partition_list(
        rd_kafka_topic_partition_list_new(tpos.size()), rd_kafka_topic_partition_list_destroy);

    for (const auto & tpo : tpos)
    {
        auto partition_offset = rd_kafka_topic_partition_list_add(topic_partition_list.get(), tpo.topic.c_str(), tpo.partition);

        /// rd_kafka_offsets_store commits `offset` as it is. We add 1 to the offset
        /// to keep the same semantic as rd_kafka_offset_store
        partition_offset->offset = tpo.offset + 1;

        LOG_INFO(log, "Stores commit offset={} for topic={} partition={}", tpo.offset, tpo.topic, tpo.partition);
    }

    auto err = rd_kafka_offsets_store(consumer_handle.get(), topic_partition_list.get());
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to commit offsets", rd_kafka_err2str(err));
        return mapErrorCode(err);
    }

    return DB::ErrorCodes::OK;
}
}
