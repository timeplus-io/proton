#pragma once

#include <boost/algorithm/string/join.hpp>

#include <string>
#include <vector>

struct rd_kafka_topic_s;

namespace DWAL
{
/// KafkaWALContext is not thread safe, so each produce/consume thread
/// shall maintain its own context instance
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

    /// Admin API settings
    int32_t partitions = 1;
    int32_t replication_factor = 1;

    /// none, gzip, snappy, lz4, zstd, inherit
    std::string compression_codec = "snappy";

    /// Enable compression for internal kafka topics and message
    bool client_side_compression = false;

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

    /// Per topic librdkafka client side settings for consumer
    int32_t consume_callback_max_messages = 100000;
    int32_t consume_callback_max_rows = 1000000;
    int32_t consume_callback_max_bytes = 33554432; /// 32 MB
    int32_t consume_callback_timeout_ms = 1000;

    /// Per topic max message size
    int32_t message_max_bytes = -1;

    static std::string topicPartitonKey(const std::string & topic, int32_t partition) { return topic + "$" + std::to_string(partition); }

    std::string key() const { return topicPartitonKey(topic, partition); }

    std::string string() const
    {
        std::vector<std::string> ctxes;
        ctxes.push_back("topic=" + topic);
        ctxes.push_back("partition=" + std::to_string(partition));
        ctxes.push_back("offset=" + std::to_string(offset));
        ctxes.push_back("partitions=" + std::to_string(partitions));
        ctxes.push_back("replication_factor=" + std::to_string(replication_factor));
        ctxes.push_back("compression_codec=" + compression_codec);
        ctxes.push_back("client_side_compression=" + std::to_string(client_side_compression));
        ctxes.push_back("retention_ms=" + std::to_string(retention_ms));
        ctxes.push_back("retention_bytes=" + std::to_string(retention_bytes));
        ctxes.push_back("segment_bytes=" + std::to_string(segment_bytes));
        ctxes.push_back("segment_ms=" + std::to_string(segment_ms));
        ctxes.push_back("flush_messages=" + std::to_string(flush_messages));
        ctxes.push_back("flush_ms=" + std::to_string(flush_ms));
        ctxes.push_back("cleanup_policy=" + cleanup_policy);
        ctxes.push_back("request_required_acks=" + std::to_string(request_required_acks));
        ctxes.push_back("request_timeout_ms=" + std::to_string(request_timeout_ms));
        ctxes.push_back("auto_offset_reset=" + auto_offset_reset);
        ctxes.push_back("consume_callback_max_messages=" + std::to_string(consume_callback_max_messages));
        ctxes.push_back("consume_callback_max_rows=" + std::to_string(consume_callback_max_rows));
        ctxes.push_back("consume_callback_max_bytes=" + std::to_string(consume_callback_max_bytes));
        ctxes.push_back("consume_callback_timeout_ms=" + std::to_string(consume_callback_timeout_ms));
        ctxes.push_back("message_max_bytes=" + std::to_string(message_max_bytes));

        return boost::algorithm::join(ctxes, " ");
    }

    /// Cached topic handle across call like for KafkaWALSimpleConsumer
    std::shared_ptr<rd_kafka_topic_s> topic_handle;
    bool topic_consuming_started = false;

    KafkaWALContext(const std::string & topic_, int32_t partition_, int64_t offset_) : topic(topic_), partition(partition_), offset(offset_) { }

    KafkaWALContext(
        const std::string & topic_, int32_t partitions_, int32_t replication_factor_, const std::string & cleanup_policy_ = "delete")
        : topic(topic_), partitions(partitions_), replication_factor(replication_factor_), cleanup_policy(cleanup_policy_)
    {
    }

    explicit KafkaWALContext(const std::string & topic_) : topic(topic_) { }
};
}
