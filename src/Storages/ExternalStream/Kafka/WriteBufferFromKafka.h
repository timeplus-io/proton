#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <KafkaLog/KafkaWALCommon.h>

namespace DB
{

class WriteBufferFromKafka final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    // WriteBufferFromKafka does not take the ownership of `topic_`.
    explicit WriteBufferFromKafka(rd_kafka_topic_t * topic_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromKafka() override = default;

    void onMessageDelivery(const rd_kafka_message_t *);

    /// the number of acknowledgement has been received so far for the current checkpoint period
    size_t acked() const { return state.acked; }
    /// the number of errors has been received so far for the current checkpoint period
    size_t error_count() const { return state.error_count; }
    /// the number of outstanding messages for the current checkpoint period
    size_t outstandings() const { return state.outstandings; }
    /// the last error code received from delivery report callback
    rd_kafka_resp_err_t lastSeenError() const { return static_cast<rd_kafka_resp_err_t>(state.last_error_code.load()); }
    /// check if there are no more outstandings (i.e. delivery reports have been recieved
    /// for all out-go messages, regardless if a message is successfully delivered or not)
    bool hasNoOutstandings() const { return state.outstandings == state.acked + state.error_count; }
    /// allows to reset the state after each checkpoint
    void resetState() { state.reset(); }

    /// Set the partition ID the buffer will write data to.
    /// This makes it possible write data to different paritions.
    void write_to_partition(Int32 id) { partition_id = id; }
private:
    void nextImpl() override;

    struct State final
    {
        std::atomic_size_t outstandings = 0;
        std::atomic_size_t acked = 0;
        std::atomic_size_t error_count = 0;
        std::atomic_int last_error_code = 0;

        void reset();
    };

    rd_kafka_topic_t * topic;
    State state;

    Int32 partition_id;
};
}
