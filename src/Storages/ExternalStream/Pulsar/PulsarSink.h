#pragma once

#include <Core/BlockWithShard.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
// #include <Storages/ExternalStream/Kafka/WriteBufferFromKafkaSink.h>
#include <Common/ThreadPool.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class PulsarSink final : public SinkToStorage
{
public:
    PulsarSink(
        const Pulsar * pulsar,
        const Block & header,
        Int32 initial_partition_cnt,
        const ASTPtr & message_key,
        ContextPtr context,
        Poco::Logger * logger_,
        ExternalStreamCounterPtr external_stream_counter_);
    ~PulsarSink() override;

    String getName() const override { return "PulsarSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:

    void addMessageToBatch(char * pos, size_t len);

    /// the number of acknowledgement has been received so far for the current checkpoint period
    size_t acked() const noexcept { return state.acked; }
    /// the number of errors has been received so far for the current checkpoint period
    size_t errorCount() const noexcept { return state.error_count; }
    /// the number of outstanding messages for the current checkpoint period
    size_t outstandings() const noexcept { return state.outstandings; }
    /// the last error code received from delivery report callback
    /// check if there are no more outstandings (i.e. delivery reports have been recieved
    /// for all out-go messages, regardless if a message is successfully delivered or not)
    bool hasOutstandingMessages() const noexcept { return state.outstandings != state.acked + state.error_count; }
    /// allows to reset the state after each checkpoint
    void resetState() { state.reset(); }

    static const int POLL_TIMEOUT_MS {500};

    Int32 partition_cnt {0};
    bool one_message_per_row {false};

    ThreadPool background_jobs {1};
    std::atomic_flag is_finished {false};

    OutputFormatPtr writer;

    ExpressionActionsPtr message_key_expr;
    String message_key_column_name;

    std::vector<StringRef> keys_for_current_batch;
    size_t current_batch_row {0};
    Int32 next_partition {0};

    struct State
    {
        std::atomic_size_t outstandings {0};
        std::atomic_size_t acked {0};
        std::atomic_size_t error_count {0};
        std::atomic_int last_error_code {0};

        void reset();
    };

    State state;

    Poco::Logger * logger;
    ExternalStreamCounterPtr external_stream_counter;
};
}
