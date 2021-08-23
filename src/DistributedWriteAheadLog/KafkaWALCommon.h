#pragma once

#include "Record.h"

#include <librdkafka/rdkafka.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace DWAL
{
struct KafkaWALStats;

int32_t mapErrorCode(rd_kafka_resp_err_t err, bool retriable = false);

/// Escape namespace `ns` and `name` to dwal's restrict char set
std::string escapeDWALName(const std::string & ns, const std::string & name);

using KConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;
using KTopicConfPtr = std::unique_ptr<rd_kafka_topic_conf_t, decltype(rd_kafka_topic_conf_destroy) *>;
using KConfCallback = std::function<void(rd_kafka_conf_t *)>;
using KConfParams = std::vector<std::pair<String, String>>;

std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)>
initRdKafkaHandle(rd_kafka_type_t type, KConfParams & params, KafkaWALStats * stats, KConfCallback cb_setup);

std::shared_ptr<rd_kafka_topic_t>
initRdKafkaTopicHandle(const std::string & topic, KConfParams & params, rd_kafka_t * rd_kafka, KafkaWALStats * stats);

RecordPtr kafkaMsgToRecord(rd_kafka_message_t * msg, bool copy_topic = false);
}
