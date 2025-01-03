#pragma once

#include "config.h"

#if USE_PULSAR

#    include <NativeLog/Base/ByteVector.h>
#    include <Interpreters/Context.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Sinks/SinkToStorage.h>
#    include <Storages/ExternalStream/ExternalStreamCounter.h>
#    include <Storages/ExternalStream/Kafka/WriteBufferFromKafkaSink.h>

#    include <pulsar/Producer.h>

namespace DB
{

namespace ExternalStream
{

class PulsarSink final : public SinkToStorage
{
public:
    PulsarSink(
        const Block & header,
        pulsar::Producer && producer,
        const String & data_format,
        const FormatSettings & format_settings,
        UInt64 max_rows_per_message,
        UInt64 max_message_size,
        ExternalStreamCounterPtr external_stream_counter,
        Poco::Logger * logger,
        const ContextPtr & context);

    ~PulsarSink() override;

    String getName() const override { return "PulsarSink"; }

    void consume(Chunk chunk) override;
    void checkpoint(CheckpointContextPtr context) override;
    void onFinish() override;

private:
    void onWriteBufferNext(char * pos, size_t len, size_t total_len);
    void tryCarryOverPendingData();
    void onSent(pulsar::Result, const pulsar::MessageId &);
    void waitForAcks(UInt64 timeout_ms = 0) const;

    pulsar::Producer producer;
    std::function<void(pulsar::Result, const pulsar::MessageId &)> send_async_callback;

    WriteBufferFromKafkaSink write_buffer;
    nlog::ByteVector pending_data;
    OutputFormatPtr writer;

    size_t rows_in_current_message;

    bool generate_message_key{false};
    size_t key_pos{0};
    ColumnPtr message_key_column;

    struct State
    {
        std::atomic_size_t outstandings{0};
        std::atomic_size_t acked{0};
        std::atomic_size_t error_count{0};
        std::atomic_int32_t last_error_code{0};

        /// allows to reset the state after each checkpoint
        void reset();
    };

    State state;

    std::atomic_flag stopped;

    ExternalStreamCounterPtr external_stream_counter;
    Poco::Logger * logger;
};

}

}

#endif
