#pragma once

#include <atomic>
#include <librdkafka/rdkafka.h>

#include "IO/BufferWithOwnMemory.h"
#include "IO/WriteBuffer.h"

namespace DB
{

using RdKafkaTopicPtr = std::unique_ptr<rd_kafka_topic_t, void (*)(rd_kafka_topic_t *)>;

class WriteBufferFromKafka : public BufferWithOwnMemory<WriteBuffer>
{
private:
    struct State
    {
        std::atomic_size_t outstandings;
        std::atomic_size_t acked;
        std::atomic_int errorCode;
    };

public:
    WriteBufferFromKafka(const Poco::Logger * logger, RdKafkaTopicPtr topic_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromKafka() override = default;

    void onDrMsg(const rd_kafka_message_t *);
    bool fullyAcked() const;
    rd_kafka_resp_err_t deliveryError() const;
    size_t outstandings() const;
    size_t acked() const;
    void resetState();

protected:
    void nextImpl() override;
    void finalizeImpl() override;

private:
    const Poco::Logger * log;
    RdKafkaTopicPtr topic;
    State state;
};
}
