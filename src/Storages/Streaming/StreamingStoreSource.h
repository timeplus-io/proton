#pragma once

#include "StreamingStoreSourceBase.h"

namespace DB
{
class StreamShard;
class StreamingBlockReaderKafka;
class StreamingBlockReaderNativeLog;

class StreamingStoreSource final : public StreamingStoreSourceBase
{
public:
    StreamingStoreSource(
        std::shared_ptr<StreamShard> stream_shard_,
        const Block & header,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        Int64 sn,
        Poco::Logger * log_);

    StreamingStoreSource();

    ~StreamingStoreSource() override = default;

    void resetSN(Int64 sn) override;

    void recover(CheckpointContextPtr ckpt_ctx_) override;

    String getName() const override { return "StreamingStoreSource"; }

    std::pair<String, Int32> getStreamShard() const override;

private:
    inline nlog::RecordPtrs read();
    void readAndProcess() override;

private:
    std::unique_ptr<StreamingBlockReaderKafka> kafka_reader;

    std::unique_ptr<StreamingBlockReaderNativeLog> nativelog_reader;

    std::atomic_flag sn_reseted;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;
};

using StreamingStoreSourcePtr = std::shared_ptr<StreamingStoreSource>;
}
