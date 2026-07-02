#pragma once

#include "config.h"

#if USE_NATSIO

#include <Processors/Streaming/ISource.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSource.h>
#include <Common/Stopwatch.h>

#include <nats.h>

namespace DB::ExternalStream
{

class NATSJetstream;

class NATSJetstreamSource final : public Streaming::ISource, public ExternalStreamSource
{
public:
    NATSJetstreamSource(
        const Block & header_,
        const StorageSnapshotPtr & storage_snapshot_,
        bool is_streaming_,
        const String & data_format,
        const FormatSettings & format_settings,
        natsSubscription * subscription_,
        std::shared_ptr<NATSJetstream> storage_,
        const String & consumer_name,
        size_t max_block_size_,
        UInt64 stall_timeout_ms,
        ExternalStreamCounterPtr counter,
        LoggerPtr logger_,
        const ContextPtr & context_);

    ~NATSJetstreamSource() override;

    String getName() const override { return "NATSJetstreamSource"; }

    /// Reports the estimated [low, high] sequence range in the stream
    std::pair<Int64, Int64> sequenceRange() const override;

    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    class StallDetector
    {
    public:
        StallDetector(UInt64 timeout_ms_, LoggerPtr logger_);

        void checkAndHandleStall(NATSJetstreamSource & source);
        void onProgress();
        void reset();

    private:
        UInt64 timeout_ms{0};
        Int64 recorded_last_processed_sn{-1};
        Stopwatch timer;
        Stopwatch caught_up_timer;
        LoggerPtr logger;
    };

    void parseFormat(natsMsg * msg);

    void doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64 sn) override;

    Strings doFetchData(const Streaming::SequenceRange &) override;

    void getPhysicalHeader() override;

    std::vector<std::function<Field(natsMsg *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    std::shared_ptr<StreamingFormatExecutor> format_executor;


    Int32 record_consume_timeout_ms{100};

    std::shared_ptr<NATSJetstream> storage;
    natsSubscription * subscription = nullptr;


    String current_subject;
    String current_consumer_name;
    Int64 messages_to_skip{0};

    bool ignore_format_errors = false;
    std::optional<Exception> consume_exception;


    MutableColumns current_batch;


    std::atomic_flag start_consume_flag;
    bool reached_the_end{false};

    StallDetector stall_detector;

    ExternalStreamCounterPtr external_stream_counter;
};

}

#endif
