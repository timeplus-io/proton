#pragma once

#include <Storages/Stream/StreamingStoreSourceBase.h>

namespace DB
{
class StreamShardStore;
class StreamingBlockReaderKafka;
class StreamingBlockReaderNativeLog;

class StreamingStoreSource final : public StreamingStoreSourceBase
{
public:
    StreamingStoreSource(
        std::shared_ptr<StreamShardStore> stream_shard_store_,
        const Block & header,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        Int64 sn,
        LoggerPtr logger_);

    StreamingStoreSource();

    ~StreamingStoreSource() override = default;

    String getName() const override { return "StreamingStoreSource"; }

    std::pair<Int64, Int64> sequenceRange() const override;

    std::pair<String, Int32> getStreamShard() const override;

private:
    void readAndProcess() override;

    Int64 lastFetchedSN() const override;

    void doResetStartSN(Int64 sn) override;

private:
    std::shared_ptr<StreamShardStore> stream_shard_store;
    StreamingBlockReaderPtr reader;
};

using StreamingStoreSourcePtr = std::shared_ptr<StreamingStoreSource>;
}
