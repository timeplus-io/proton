#include "WriteBufferFromKafka.h"
#include <cmath>
#include <utility>
#include "Common/logger_useful.h"

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
}

WriteBufferFromKafka::WriteBufferFromKafka(const Poco::Logger * logger, RdKafkaTopicPtr topic_, size_t buffer_size)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size), log(logger), topic(std::move(topic_))
{
}

void WriteBufferFromKafka::nextImpl()
{
    if (!offset())
        return;

    auto err = rd_kafka_produce(
        topic.get(),
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
        working_buffer.begin(),
        offset(),
        nullptr /* key */,
        0 /* keylen */,
        static_cast<void *>(this) /* opaque */);

    if (unlikely(err))
        throw Exception(
            "Cannot write to kafka topic at offset " + std::to_string(count()) + ", error: " + rd_kafka_err2str(rd_kafka_last_error()),
            ErrorCodes::CANNOT_WRITE_TO_KAFKA);

    ++state.outstandings;
}

void WriteBufferFromKafka::finalizeImpl()
{
    LOG_INFO(log, "finalizeImpl is called, offset={}", offset());
}

void WriteBufferFromKafka::onDrMsg(const rd_kafka_message_t * msg)
{
    if (msg->err)
        state.errorCode.store(msg->err);
    else
        ++state.acked;
}

bool WriteBufferFromKafka::fullyAcked() const
{
    return state.acked == state.outstandings;
}

rd_kafka_resp_err_t WriteBufferFromKafka::deliveryError() const
{
    return static_cast<rd_kafka_resp_err_t>(state.errorCode.load());
}

size_t WriteBufferFromKafka::outstandings() const
{
    return state.outstandings.load();
}
size_t WriteBufferFromKafka::acked() const
{
    return state.acked.load();
}

void WriteBufferFromKafka::resetState()
{
    state.outstandings.store(0);
    state.acked.store(0);
    state.errorCode.store(0);
}
}
