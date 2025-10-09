#pragma once

#include <Storages/Stream/IngestMode.h>

#include <Cluster/KafkaLog/Results.h>
#include <Columns/IColumn.h>
#include <Core/BlockWithShard.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Streaming/SizedChunkSplitter.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class Context;
class StorageStream;

class StreamSink final : public SinkToStorage
{
public:
    StreamSink(std::shared_ptr<StorageStream> storage_, StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);
    ~StreamSink() override = default;

    void consume(Chunk chunk) override;
    void onFinish() override;
    String getName() const override { return "StreamSink"; }

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;

private:
    IngestMode getIngestMode() const;
    UInt64 getMaxInsertBlockBytes() const;

    BlocksWithShard shardBlock(Block block) const;

    const std::vector<UInt16> & getColumnPositions(UInt16 version_requested);

private:
    std::shared_ptr<StorageStream> storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;
    IngestMode ingest_mode;

    std::pair<UInt16, std::vector<UInt16>> versioned_column_positions{0, {}};

    /// For writeCallback
    std::atomic_uint64_t outstanding = 0;
    std::optional<ThreadPool> pool;

    struct IngestState
    {
        std::atomic_uint64_t committed = 0;
        std::atomic_int32_t errcode = 0;
    };
    std::shared_ptr<IngestState> ingest_state;

    Streaming::SizedChunkSplitter chunk_splitter;

    /// Track the consume chunk index for generating unique idempotent key
    UInt32 chunk_id = 0;

    std::mutex mutex;
    std::condition_variable checkpoint_cv;
};

}
