#pragma once

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <fmt/format.h>

namespace klog
{
struct KafkaWALAuth
{
    std::string security_protocol;
    std::string username;
    std::string password;
    std::string sasl_mechanism;
    std::string ssl_ca_cert_file;

    bool operator==(const KafkaWALAuth & o) const
    {
        return security_protocol == o.security_protocol
            && username == o.username
            && password == o.password
            && ssl_ca_cert_file == o.ssl_ca_cert_file;
    }

    bool usesSASL() const
    {
        return boost::istarts_with(security_protocol, "SASL_");
    }

    /// "SASL_SSL" or "SSL"
    bool usesSecureConnection() const
    {
        return boost::iends_with(security_protocol, "SSL");
    }

    void populateConfigs(std::vector<std::pair<std::string, std::string>> & params) const
    {
        params.emplace_back("security.protocol", security_protocol);
        if (usesSASL())
        {
            params.emplace_back("sasl.mechanism", sasl_mechanism);
            params.emplace_back("sasl.username", username);
            params.emplace_back("sasl.password", password);
        }

        if (usesSecureConnection() && !ssl_ca_cert_file.empty())
            params.emplace_back("ssl.ca.location", ssl_ca_cert_file);
        }
};

struct KafkaWALSettings
{
    std::string cluster_id;

    /// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    /// Global settings for both producer and consumer /// global metrics
    /// comma separated host/port: host1:port,host2:port,...
    std::string brokers;
    KafkaWALAuth auth = { .security_protocol = "plaintext" };

    int32_t message_max_bytes = 1000000;
    int32_t topic_metadata_refresh_interval_ms = 300000;
    int32_t statistic_internal_ms = 30000;
    std::string debug;

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

        settings.push_back(fmt::format("cluster_id={}", cluster_id));
        settings.push_back(fmt::format("message_max_bytes={}", message_max_bytes));
        settings.push_back(fmt::format("topic_metadata_refresh_interval_ms={}", topic_metadata_refresh_interval_ms));
        settings.push_back(fmt::format("enable_idempotence={}", enable_idempotence));
        settings.push_back(fmt::format("queue_buffering_max_messages={}", queue_buffering_max_messages));
        settings.push_back(fmt::format("queue_buffering_max_kbytes={}", queue_buffering_max_kbytes));
        settings.push_back(fmt::format("queue_buffering_max_ms={}", queue_buffering_max_ms));
        settings.push_back(fmt::format("message_send_max_retries={}", message_send_max_retries));
        settings.push_back(fmt::format("retry_backoff_ms={}", retry_backoff_ms));
        settings.push_back(fmt::format("compression_codec={}", compression_codec));
        settings.push_back(fmt::format("message_timeout_ms={}", message_timeout_ms));
        settings.push_back(fmt::format("message_delivery_async_poll_ms={}", message_delivery_async_poll_ms));
        settings.push_back(fmt::format("message_delivery_sync_poll_ms={}", message_delivery_sync_poll_ms));
        settings.push_back(fmt::format("enable_auto_commit={}", enable_auto_commit));
        settings.push_back(fmt::format("check_crcs={}", check_crcs));
        settings.push_back(fmt::format("auto_commit_interval_ms={}", auto_commit_interval_ms));
        settings.push_back(fmt::format("fetch_message_max_bytes={}", fetch_message_max_bytes));
        settings.push_back(fmt::format("fetch_wait_max_ms={}", fetch_wait_max_ms));
        settings.push_back(fmt::format("queued_min_messages={}", queued_min_messages));
        settings.push_back(fmt::format("queued_max_messages_kbytes={}", queued_max_messages_kbytes));
        settings.push_back(fmt::format("shared_subscription_flush_threshold_count={}", shared_subscription_flush_threshold_count));
        settings.push_back(fmt::format("shared_subscription_flush_threshold_bytes={}", shared_subscription_flush_threshold_bytes));
        settings.push_back(fmt::format("shared_subscription_flush_threshold_ms={}", shared_subscription_flush_threshold_ms));
        settings.push_back(fmt::format("auth.security.protocol={}", auth.security_protocol));
        settings.push_back(fmt::format("auth.sasl.mechanism={}", auth.sasl_mechanism));
        settings.push_back(fmt::format("auth.username={}", auth.username));
        settings.push_back(fmt::format("auth.password={}", auth.password));
        settings.push_back(fmt::format("auth.ssl.ca.location={}", auth.ssl_ca_cert_file));

        return boost::algorithm::join(settings, " ");
    }
};
}
