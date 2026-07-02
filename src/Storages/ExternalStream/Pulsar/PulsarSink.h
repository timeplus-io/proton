#pragma once

#include "config.h"

#if USE_PULSAR

#    include <Cluster/Base/ByteVector.h>
#    include <Interpreters/Context_fwd.h>
#    include <Processors/Executors/MessageQueueFormatExecutor.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Sinks/SinkToStorage.h>
#    include <Storages/ExternalStream/ExternalStreamCounter.h>

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
        LoggerPtr logger,
        const ContextPtr & context);

    ~PulsarSink() override;

    String getName() const override { return "PulsarSink"; }

    void consume(Chunk chunk) override;
    void doCheckpoint(CheckpointContextPtr context) override;
    void onFinish() override;

private:
    void onMessageReady(const String & message, ColumnPtr key_col, ColumnPtr headers_col, size_t row);
    void onSent(pulsar::Result, const pulsar::MessageId &);
    void waitForAcks() const;

    pulsar::Producer producer;
    std::function<void(pulsar::Result, const pulsar::MessageId &)> send_async_callback;

    std::optional<size_t> msg_key_pos;
    std::optional<size_t> msg_headers_pos;

    std::unique_ptr<MessageQueueFormatExecutor> format_executor;

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

    UInt64 ack_timeout_ms{0};
    std::atomic_flag stopped;

    ExternalStreamCounterPtr external_stream_counter;
    LoggerPtr logger;
};

}

}

#endif
