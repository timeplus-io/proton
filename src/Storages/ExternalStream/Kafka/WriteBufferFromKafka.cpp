#include "WriteBufferFromKafka.h"

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
}

WriteBufferFromKafka::WriteBufferFromKafka(rd_kafka_topic_t * topic_, size_t buffer_size)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size), topic(topic_)
{
}

void WriteBufferFromKafka::nextImpl()
{
    if (!offset())
        return;

    auto err = rd_kafka_produce(
        topic,
        /// we want to trigger the partitioner function, check KafkaSink.cpp
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
        working_buffer.begin(),
        offset(),
        "" /* key, even though we don't use the key, but it's needed to trigger the partitioner_cb */,
        0 /* keylen */,
        reinterpret_cast<void *>(partition_id) /* opaque */);

    if (unlikely(err))
        throw Exception(
            "Cannot write to kafka topic at offset " + std::to_string(count()) + ", error: " + rd_kafka_err2str(rd_kafka_last_error()),
            ErrorCodes::CANNOT_WRITE_TO_KAFKA);

    ++state.outstandings;
}

void WriteBufferFromKafka::onMessageDelivery(const rd_kafka_message_t * msg)
{
    if (msg->err)
    {
        state.last_error_code.store(msg->err);
        ++state.error_count;
    }
    else
        ++state.acked;
}

void WriteBufferFromKafka::State::reset()
{
    outstandings.store(0);
    acked.store(0);
    error_count.store(0);
    last_error_code.store(0);
}
}
