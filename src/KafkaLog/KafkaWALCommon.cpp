#include "KafkaWALCommon.h"
#include "KafkaWALContext.h"
#include "KafkaWALStats.h"

#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/hex.h>
#include <Common/parseIntStrict.h>

#include <cstring>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int DWAL_FATAL_ERROR;
    extern const int DWAL_RETRIABLE_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int MSG_SIZE_TOO_LARGE;
    extern const int INTERNAL_INGEST_BUFFER_FULL;
    extern const int INVALID_REPLICATION_FACTOR;
}

/// Allowed chars are ASCII alphanumerics, '.', '_' and '-'. '_' is used as escaped char in the form '_xx' where xx
/// is the hexadecimal value of the byte(s) needed to represent an illegal char in utf8.
std::string escapeName(const std::string & s)
{
    std::string escaped;
    escaped.reserve(s.size());

    for (const auto & b : s)
    {
        if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || (b == '.' || b == '-'))
        {
            escaped += b;
        }
        else
        {
            char out[4] = {"_"};
            writeHexByteUppercase(b, out + 1);
            escaped += out;
        }
    }
    return escaped;
}
}

namespace klog
{
std::string escapeTopicName(const std::string & ns, const std::string & name)
{
    return fmt::format("{}.{}", DB::escapeName(ns), DB::escapeName(name));
}

int32_t mapErrorCode(rd_kafka_resp_err_t err, bool retriable)
{
    if (retriable)
    {
        return DB::ErrorCodes::DWAL_RETRIABLE_ERROR;
    }

    /// FIXME, more code mapping
    switch (err)
    {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            return DB::ErrorCodes::OK;

        case RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS:
            return DB::ErrorCodes::RESOURCE_ALREADY_EXISTS;

        case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
            /// fallthrough
        case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
            /// fallthrough
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
            return DB::ErrorCodes::RESOURCE_NOT_FOUND;

        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            return DB::ErrorCodes::BAD_ARGUMENTS;

        case RD_KAFKA_RESP_ERR__FATAL:
            throw DB::Exception("Fatal error occurred, shall tear down the whole program", DB::ErrorCodes::DWAL_FATAL_ERROR);

        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
            return DB::ErrorCodes::MSG_SIZE_TOO_LARGE;

        case RD_KAFKA_RESP_ERR__QUEUE_FULL:
            return DB::ErrorCodes::INTERNAL_INGEST_BUFFER_FULL;

        case RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR:
            return DB::ErrorCodes::INVALID_REPLICATION_FACTOR;

        default:
            return DB::ErrorCodes::UNKNOWN_EXCEPTION;
    }
}

std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)>
initRdKafkaHandle(rd_kafka_type_t type, KConfParams & params, KafkaWALStats * stats, KConfCallback cb_setup)
{
    KConfPtr kconf{rd_kafka_conf_new(), rd_kafka_conf_destroy};
    if (!kconf)
    {
        LOG_ERROR(stats->log, "Failed to create kafka conf, error={}", rd_kafka_err2str(rd_kafka_last_error()));
        throw DB::Exception("Failed to create kafka conf", mapErrorCode(rd_kafka_last_error()));
    }

    char errstr[512] = {'\0'};
    for (const auto & param : params)
    {
        auto ret = rd_kafka_conf_set(kconf.get(), param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(stats->log, "Failed to set kafka param_name={} param_value={} error={}", param.first, param.second, ret);
            throw DB::Exception("Failed to create kafka conf", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    if (cb_setup)
        cb_setup(kconf.get());

    rd_kafka_conf_set_opaque(kconf.get(), stats);

    std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)> kafka_handle(
        rd_kafka_new(type, kconf.release(), errstr, sizeof(errstr)), rd_kafka_destroy);
    if (!kafka_handle)
    {
        LOG_ERROR(stats->log, "Failed to create kafka handle, error={}", errstr);
        throw DB::Exception("Failed to create kafka handle", mapErrorCode(rd_kafka_last_error()));
    }

    return kafka_handle;
}

std::shared_ptr<rd_kafka_topic_t>
initRdKafkaTopicHandle(const std::string & topic, KConfParams & params, rd_kafka_t * rd_kafka, KafkaWALStats * stats)
{
    KTopicConfPtr tconf{rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy};
    if (!tconf)
    {
        LOG_ERROR(stats->log, "Failed to create kafka topic conf, error={}", rd_kafka_err2str(rd_kafka_last_error()));
        throw DB::Exception("Failed to created underlying streaming store conf", mapErrorCode(rd_kafka_last_error()));
    }

    char errstr[512] = {'\0'};
    for (const auto & param : params)
    {
        auto ret = rd_kafka_topic_conf_set(tconf.get(), param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(
                stats->log,
                "Failed to set kafka topic param, topic={} param_name={} param_value={} error={}",
                topic,
                param.first,
                param.second,
                errstr);
            throw DB::Exception("Failed to set underlying streaming store param", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    rd_kafka_topic_conf_set_opaque(tconf.get(), stats);

    std::shared_ptr<rd_kafka_topic_t> topic_handle{rd_kafka_topic_new(rd_kafka, topic.c_str(), tconf.release()), rd_kafka_topic_destroy};
    if (!topic_handle)
    {
        LOG_ERROR(stats->log, "Failed to create kafka topic handle, topic={} error={}", topic, rd_kafka_err2str(rd_kafka_last_error()));
        throw DB::Exception("Failed to create underlying streaming store handle", mapErrorCode(rd_kafka_last_error()));
    }

    return topic_handle;
}

nlog::RecordPtr kafkaMsgToRecord(rd_kafka_message_t * msg, const nlog::SchemaContext & schema_ctx, bool copy_topic)
{
    assert(msg != nullptr);

    auto consume_time = DB::UTCMilliseconds::now();
    nlog::RecordPtr record = nlog::Record::deserialize(static_cast<const char *>(msg->payload), msg->len, schema_ctx);

    if (unlikely(!record))
        return nullptr;

    record->setSN(msg->offset);
    record->setShard(msg->partition);
    /// Override append time
    record->setAppendTime(rd_kafka_message_timestamp(msg, nullptr));
    record->setConsumeTime(consume_time);
    if (copy_topic)
        record->setStream(rd_kafka_topic_name(msg->rkt));

    rd_kafka_headers_t * hdrs = nullptr;
    if (rd_kafka_message_headers(msg, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        const void * value = nullptr;
        size_t size = 0;

        /// We only honor tp related headers for now
        rd_kafka_header_get_last(hdrs, nlog::Record::INGEST_TIME_KEY.c_str(), &value, &size);

        /// We add one more byte to the size to workaround the boundary issue in string_view
        record->setIngestTime(
            DB::parseIntStrict<int64_t>(std::string_view(static_cast<const char *>(value), size + 1), 0, size));
    }

    return record;
}

DescribeResult describeTopic(const String & name, struct rd_kafka_s * rk, Poco::Logger * log)
{
    std::shared_ptr<rd_kafka_topic_t> topic_handle{rd_kafka_topic_new(rk, name.c_str(), nullptr), rd_kafka_topic_destroy};

    if (!topic_handle)
    {
        LOG_ERROR(log, "Failed to describe topic, can't create topic handle");
        return {.err = DB::ErrorCodes::UNKNOWN_EXCEPTION};
    }

    const struct rd_kafka_metadata * metadata = nullptr;

    auto err = rd_kafka_metadata(rk, 0, topic_handle.get(), &metadata, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to describe topic, error={}", rd_kafka_err2str(err));
        return {.err = mapErrorCode(err)};
    }

    for (int32_t i = 0; i < metadata->topic_cnt; ++i)
    {
        if (name == metadata->topics[i].topic)
        {
            auto partition_cnt = metadata->topics[i].partition_cnt;
            rd_kafka_metadata_destroy(metadata);

            if (partition_cnt > 0)
                return {.err = DB::ErrorCodes::OK, .partitions = partition_cnt};
            else
                return {.err = DB::ErrorCodes::RESOURCE_NOT_FOUND};
        }
    }

    rd_kafka_metadata_destroy(metadata);
    return {.err = DB::ErrorCodes::RESOURCE_NOT_FOUND};
}

std::string boolToString(bool val)
{
    return val ? "true" : "false";
}
}
