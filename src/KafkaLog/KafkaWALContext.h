#pragma once

#include "NativeLog/Record/SchemaProvider.h"

#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>

#include <string>
#include <vector>

struct rd_kafka_topic_s;

namespace klog
{
/// KafkaWALContext is per designed per topic and is not thread safe, so each produce/consume thread
/// shall maintain its own context instance.
struct KafkaWALContext
{
    std::string topic;
    int32_t partition = 0;

    /// - absolute offset (0..N)
    /// - RD_KAFKA_OFFSET_BEGINNING, -2
    /// - RD_KAFKA_OFFSET_END, -1
    /// - RD_KAFKA_OFFSET_STORED, -1000
    /// - RD_KAFKA_OFFSET_TAIL
    int64_t offset = -1000;

    /// Enforce consuming from the offset instead of from the last ckpted offset
    bool enforce_offset = false;

    /// Admin API settings
    int32_t partitions = 1;
    int32_t replication_factor = 1;

    /// none, gzip, snappy, lz4, zstd, inherit
    std::string compression_codec = "snappy";

    /// Enable compression for internal kafka topics and message
    bool client_side_compression = false;

    /// When producing a record to Kafka broker, what kind of timestamp to use
    /// CreationTime (application assigned) or LogAppendTime (Kafka broker assigned)
    bool use_append_timestamp = true;

    /// Data retention for cleanup_policy `delete`
    int64_t retention_ms = -1;
    int64_t retention_bytes = -1;

    /// Segments roll over size
    int64_t segment_bytes = -1;

    /// Segments roll over time
    int64_t segment_ms = -1;

    /// Call fsync per flush_messages
    int64_t flush_messages = -1;

    /// Call fsync every flush_ms
    int64_t flush_ms = -1;

    /// `compact` or `delete`
    std::string cleanup_policy = "delete";

    /// Per topic producer settings
    int32_t request_required_acks = 1;
    int32_t request_timeout_ms = 30000;

    /// Per topic consumer settings
    std::string auto_offset_reset = "earliest";

    ///  When reading from Kafka, max wait time
    int32_t fetch_wait_max_ms = 100;

    /// When reading from Kafka, max bytes to fetch per read
    int32_t fetch_max_bytes =  52428800;

    /// Per topic librdkafka client side settings for consumer

    /// Minimum number of messages per topic partition to buffer on librdkafka client queue
    int64_t client_queued_min_message = 100000;
    /// Maximum bytes per topic partition to buffer on librdkafka client queue
    int64_t client_queued_max_bytes = 1024 * 1024 * 1024;

    int32_t consume_callback_max_messages = 0;
    int32_t consume_callback_max_rows = 1000000;
    int32_t consume_callback_max_bytes = 33554432; /// 32 MB
    int32_t consume_callback_timeout_ms = 1000;

    /// Per topic max message size
    int32_t message_max_bytes = -1;

    nlog::SchemaContext schema_ctx;

    static std::string topicPartitonKey(const std::string & topic, int32_t partition) { return topic + "$" + std::to_string(partition); }

    std::string key() const { return topicPartitonKey(topic, partition); }

    std::string string() const
    {
        std::vector<std::string> ctxes;
        ctxes.push_back(fmt::format("topic={}", topic));
        ctxes.push_back(fmt::format("partition={}", partition));
        ctxes.push_back(fmt::format("offset={}", offset));
        ctxes.push_back(fmt::format("partitions={}", partitions));
        ctxes.push_back(fmt::format("replication_factor={}", replication_factor));
        ctxes.push_back(fmt::format("compression_codec={}", compression_codec));
        ctxes.push_back(fmt::format("client_side_compression={}", client_side_compression));
        ctxes.push_back(fmt::format("retention_ms={}", retention_ms));
        ctxes.push_back(fmt::format("retention_bytes={}", retention_bytes));
        ctxes.push_back(fmt::format("segment_bytes={}", segment_bytes));
        ctxes.push_back(fmt::format("segment_ms={}", segment_ms));
        ctxes.push_back(fmt::format("flush_messages={}", flush_messages));
        ctxes.push_back(fmt::format("flush_ms={}", flush_ms));
        ctxes.push_back(fmt::format("cleanup_policy={}", cleanup_policy));
        ctxes.push_back(fmt::format("request_required_acks={}", request_required_acks));
        ctxes.push_back(fmt::format("request_timeout_ms={}", request_timeout_ms));
        ctxes.push_back(fmt::format("auto_offset_reset={}", auto_offset_reset));
        ctxes.push_back(fmt::format("fetch_wait_max_ms={}", fetch_wait_max_ms));
        ctxes.push_back(fmt::format("fetch_max_bytes={}", fetch_max_bytes));
        ctxes.push_back(fmt::format("client_queued_min_message={}", client_queued_min_message));
        ctxes.push_back(fmt::format("client_queued_max_bytes={}", client_queued_max_bytes));
        ctxes.push_back(fmt::format("consume_callback_max_messages={}", consume_callback_max_messages));
        ctxes.push_back(fmt::format("consume_callback_max_rows={}", consume_callback_max_rows));
        ctxes.push_back(fmt::format("consume_callback_max_bytes={}", consume_callback_max_bytes));
        ctxes.push_back(fmt::format("consume_callback_timeout_ms={}", consume_callback_timeout_ms));
        ctxes.push_back(fmt::format("message_max_bytes={}", message_max_bytes));

        return boost::algorithm::join(ctxes, " ");
    }

    /// Cached topic handle across call like for KafkaWALSimpleConsumer
    std::shared_ptr<rd_kafka_topic_s> topic_handle;
    bool topic_consuming_started = false;

    KafkaWALContext(const std::string & topic_, int32_t partition_, int64_t offset_)
        : topic(topic_), partition(partition_), offset(offset_)
    {
    }

    KafkaWALContext(
        const std::string & topic_, int32_t partitions_, int32_t replication_factor_, const std::string & cleanup_policy_ = "delete")
        : topic(topic_)
        , partitions(partitions_)
        , replication_factor(replication_factor_)
        , cleanup_policy(cleanup_policy_)
    {
    }

    explicit KafkaWALContext(const std::string & topic_) : topic(topic_) { }
};
}
