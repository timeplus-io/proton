#include <IO/Kafka/mapErrorCode.h>

#include <Common/Exception.h>

#include <librdkafka/rdkafka.h>

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
extern const int INVALID_LOGSTORE_REPLICATION_FACTOR;
extern const int TIMEOUT_EXCEEDED;
extern const int NETWORK_ERROR;
}

namespace Kafka
{

int32_t mapErrorCode(rd_kafka_resp_err_t err, bool retriable)
{
    if (retriable)
        return DB::ErrorCodes::DWAL_RETRIABLE_ERROR;

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
            throw DB::Exception(DB::ErrorCodes::DWAL_FATAL_ERROR, "Fatal error occurred, shall tear down the whole program");

        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
            return DB::ErrorCodes::MSG_SIZE_TOO_LARGE;

        case RD_KAFKA_RESP_ERR__QUEUE_FULL:
            return DB::ErrorCodes::INTERNAL_INGEST_BUFFER_FULL;

        case RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR:
            return DB::ErrorCodes::INVALID_LOGSTORE_REPLICATION_FACTOR;

        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            return DB::ErrorCodes::TIMEOUT_EXCEEDED;

        case RD_KAFKA_RESP_ERR__TRANSPORT:
            return DB::ErrorCodes::NETWORK_ERROR;

        default:
            return DB::ErrorCodes::UNKNOWN_EXCEPTION;
    }
}

}

}
