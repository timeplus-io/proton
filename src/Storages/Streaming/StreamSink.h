#pragma once

#include <Storages/Streaming/IngestMode.h>

#include <KafkaLog/Results.h>
#include <NativeLog/Requests/AppendRequest.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Core/DataBlockWithShard.h>

namespace DB
{
class Context;
class StorageStream;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class StreamSink final : public SinkToStorage
{
public:
    StreamSink(StorageStream & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);
    ~StreamSink() override = default;

    void consume(Chunk chunk) override;
    void onFinish() override;
    String getName() const override { return "StreamSink"; }

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;

private:
    BlocksWithShard shardBlock(Block block) const;
    BlocksWithShard doShardBlock(Block block) const;
    IngestMode getIngestMode() const;

private:
    StorageStream & storage;
    StorageMetadataPtr metadata_snapshot;
    /// StorageSnapshotPtr storage_snapshot;
    ContextPtr query_context;

    std::vector<UInt16> column_positions;

    /// For writeCallback
    std::uint64_t outstanding = 0;

    struct IngestState
    {
        std::atomic_uint64_t committed = 0;
        std::atomic_int32_t errcode = 0;
    };
    std::shared_ptr<IngestState> ingest_state;

    std::mutex mutex;
    std::condition_variable checkpoint_cv;
};

}
