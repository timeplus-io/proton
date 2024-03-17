#pragma once

#include "Results.h"

#include <NativeLog/Record/Record.h>

#include <librdkafka/rdkafka.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace Poco
{
class Logger;
}

namespace klog
{
struct KafkaWALStats;
struct KafkaWALContext;

int32_t mapErrorCode(rd_kafka_resp_err_t err, bool retriable = false);

using KafkaPtr = std::unique_ptr<rd_kafka_t, decltype(rd_kafka_destroy) *>;
using KTopicPtr = std::unique_ptr<rd_kafka_topic_t, decltype(rd_kafka_topic_destroy) *>;
using KConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;
using KTopicConfPtr = std::unique_ptr<rd_kafka_topic_conf_t, decltype(rd_kafka_topic_conf_destroy) *>;
using KConfCallback = std::function<void(rd_kafka_conf_t *)>;

using KConfParams = std::vector<std::pair<std::string, std::string>>;

struct PartitionTimestamp
{
    PartitionTimestamp(int32_t partition_, int64_t timestamp_) : partition(partition_), timestamp(timestamp_) { }

    int32_t partition;
    int64_t timestamp;
};

std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)>
initRdKafkaHandle(rd_kafka_type_t type, KConfParams & params, KafkaWALStats * stats, KConfCallback cb_setup);

std::shared_ptr<rd_kafka_topic_t>
initRdKafkaTopicHandle(const std::string & topic, KConfParams & params, rd_kafka_t * rd_kafka, KafkaWALStats * stats);

nlog::RecordPtr kafkaMsgToRecord(rd_kafka_message_t * msg, const nlog::SchemaContext & schema_ctx, bool copy_topic = false);

DescribeResult describeTopic(const String & name, struct rd_kafka_s * rk, Poco::Logger * log);
DescribeResult describeTopic(rd_kafka_topic_t * rkt, struct rd_kafka_s * rk, Poco::Logger * log);

std::vector<int64_t> getOffsetsForTimestamps(
    struct rd_kafka_s * rd_handle,
    const std::string & topic,
    const std::vector<PartitionTimestamp> & partition_timestamps,
    int32_t timeout_ms);

std::string boolToString(bool val);
}
