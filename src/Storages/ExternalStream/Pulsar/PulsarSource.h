#pragma once

#include "config.h"

#if USE_PULSAR

#    include <Processors/Executors/StreamingFormatExecutor.h>
#    include <Processors/Streaming/ISource.h>
#    include <Storages/ExternalStream/ExternalStreamCounter.h>
#    include <Storages/StorageSnapshot.h>

#    include <pulsar/Consumer.h>
#    include <pulsar/Reader.h>

namespace DB
{

namespace ExternalStream
{

class PulsarSource final : public Streaming::ISource
{
public:
    PulsarSource(
        const Block & header_,
        std::map<size_t, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>> virtual_header_,
        bool is_streaming,
        std::unique_ptr<ReadBuffer> read_buffer_,
        std::unique_ptr<StreamingFormatExecutor> format_executor_,
        pulsar::Reader && reader_,
        ExternalStreamCounterPtr counter,
        Poco::Logger * logger_,
        const ContextPtr & context_);

    ~PulsarSource() override;

    String getName() const override { return "PulsarSource"; }
    String description() const override { return fmt::format("topic={}", getTopic()); }

    Chunk generate() override;

protected:
    void onCancel() override;

private:
    const String & getTopic() const { return reader.getTopic(); }

    Chunk generateWithConsumer();
    Chunk generateWithReader();

    /// Checkpointing
    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64) override {
        /// Since Streaming::ISource will always set the last_processed_sn to a proper value,
        /// we don't need to do anything special here.
    }

    /// Virutal columns' positions and their types, and value calculation functions.
    std::map<size_t, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>> virtual_header;
    Chunk header_chunk;

    Int64 generate_timeout_ms{100};
    /// read_buffer is a dependency of format_executor, make sure we keep them in order.
    std::unique_ptr<ReadBuffer> read_buffer;
    std::unique_ptr<StreamingFormatExecutor> format_executor;

    pulsar::Reader reader;

    /// Number of messages to skip at the beginning. This is only used during MV auto auto-recovery with best-effort policy.
    Int64 messages_to_skip{0};
    /// The message ID of the latest consumed message.
    std::optional<pulsar::MessageId> latest_consumed_message_id;

    /// The message ID of the last message the source should read.
    /// Once reach this ID, the source will stop reading more messages.
    /// Only used for non-streaming queries.
    pulsar::MessageId end_message_id;
    bool is_finished{false};

    size_t max_block_size{0};

    ExternalStreamCounterPtr external_stream_counter;

    Poco::Logger * logger;
};

}

}

#endif
