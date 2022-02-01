#pragma once

#include <boost/algorithm/string/join.hpp>

#include <string>
#include <vector>

namespace DWAL
{
struct KafkaWALSettings
{
    std::string cluster_id;

    /// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    /// Global settings for both producer and consumer /// global metrics
    /// comma separated host/port: host1:port,host2:port,...
    std::string brokers;
    std::string security_protocol = "plaintext";
    /// FIXME, SASL, SSL etc support

    int32_t message_max_bytes = 1000000;
    int32_t topic_metadata_refresh_interval_ms = 300000;
    int32_t statistic_internal_ms = 30000;
    std::string debug = "";

    /////////////////////////////////////////////////////

    /// Global settings for producer
    /// std::string transactional_id;
    /// int32_t transaction_timeout_ms = 60000;
    bool enable_idempotence = true;
    int32_t queue_buffering_max_messages = 100000;
    int32_t queue_buffering_max_kbytes = 1048576;
    /// queue_buffering_max_ms impacts the message producing latency
    /// https://github.com/edenhill/librdkafka/wiki/How-to-decrease-message-latency
    int32_t queue_buffering_max_ms = 50; /// same as linger.ms
    int32_t message_send_max_retries = 2;
    int32_t retry_backoff_ms = 100;
    /// none, gzip, snappy, lz4, zstd, inherit
    std::string compression_codec = "snappy";

    /// Global librdkafka client side settings for producer
    int32_t message_timeout_ms = 40000;
    int32_t message_delivery_async_poll_ms = 100;
    int32_t message_delivery_sync_poll_ms = 10;

    /////////////////////////////////////////////////////

    /// Global settings for consumer
    std::string group_id;
    /// std::string group_instance_id
    /// std::string partition_assignment_strategy

    /// Global consumer settings, but can override for a specific topic
    std::string auto_offset_reset = "earliest";

    int32_t session_timeout_ms = 10000;
    int32_t max_poll_interval_ms = 30000;

    bool enable_auto_commit = true;
    bool check_crcs = false;
    int32_t auto_commit_interval_ms = 5000;
    int32_t fetch_message_max_bytes = 1048576;
    /// Max time the broker may wait to fetch.min.bytes of messages
    /// This setting impacts the message consuming latency
    int32_t fetch_wait_max_ms = 500;

    /// Global librdkafka client side settings for consumer per topic+partition
    int32_t queued_min_messages = 1000000;
    int32_t queued_max_messages_kbytes = 65536;

    /// Global settings for shared subscription
    int32_t shared_subscription_flush_threshold_count = 10000;
    int32_t shared_subscription_flush_threshold_bytes = 10 * 1024 * 1024;
    int32_t shared_subscription_flush_threshold_ms = 1000;

    std::unique_ptr<KafkaWALSettings> clone() const { return std::make_unique<KafkaWALSettings>(*this); }

    std::string string() const
    {
        std::vector<std::string> settings;

        settings.push_back("cluster_id=" + cluster_id);
        settings.push_back("message_max_bytes=" + std::to_string(message_max_bytes));
        settings.push_back("topic_metadata_refresh_interval_ms=" + std::to_string(topic_metadata_refresh_interval_ms));
        settings.push_back("enable_idempotence=" + std::to_string(enable_idempotence));
        settings.push_back("queue_buffering_max_messages=" + std::to_string(queue_buffering_max_messages));
        settings.push_back("queue_buffering_max_kbytes=" + std::to_string(queue_buffering_max_kbytes));
        settings.push_back("queue_buffering_max_ms=" + std::to_string(queue_buffering_max_ms));
        settings.push_back("message_send_max_retries=" + std::to_string(message_send_max_retries));
        settings.push_back("retry_backoff_ms=" + std::to_string(retry_backoff_ms));
        settings.push_back("compression_codec=" + compression_codec);
        settings.push_back("message_timeout_ms=" + std::to_string(message_timeout_ms));
        settings.push_back("message_delivery_async_poll_ms=" + std::to_string(message_delivery_async_poll_ms));
        settings.push_back("message_delivery_sync_poll_ms=" + std::to_string(message_delivery_sync_poll_ms));
        settings.push_back("enable_auto_commit=" + std::to_string(enable_auto_commit));
        settings.push_back("check_crcs=" + std::to_string(check_crcs));
        settings.push_back("auto_commit_interval_ms=" + std::to_string(auto_commit_interval_ms));
        settings.push_back("queued_min_messages=" + std::to_string(queued_min_messages));
        settings.push_back("queued_max_messages_kbytes=" + std::to_string(queued_max_messages_kbytes));
        settings.push_back("shared_subscription_flush_threshold_count=" + std::to_string(shared_subscription_flush_threshold_count));
        settings.push_back("shared_subscription_flush_threshold_bytes=" + std::to_string(shared_subscription_flush_threshold_bytes));
        settings.push_back("shared_subscription_flush_threshold_ms=" + std::to_string(shared_subscription_flush_threshold_ms));

        return boost::algorithm::join(settings, " ");
    }
};
}
