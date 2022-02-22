#pragma once

#include <DistributedWALClient/Record.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{
class StreamingStoreSourceMultiplexer;

class StreamingStoreSourceChannel final : public SourceWithProgress
{
public:
    StreamingStoreSourceChannel(
        std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
        Block header,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr query_context_);

    ~StreamingStoreSourceChannel() override;

    String getName() const override { return "StreamingStoreSourceChannel"; }

    UInt32 getID() const { return id; }

    Chunk generate() override;

    void add(DWAL::RecordPtrs records);

private:
    void readAndProcess();
    void calculateColumnPositions(const Block & header, const Block & schema);

private:
    static std::atomic<uint32_t> sequence_id;

    UInt32 id;
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer;

    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    /// FIXME, use another lock-free one?
    ConcurrentBoundedQueue<DWAL::RecordPtrs> records_queue;

    Chunk header_chunk;
    std::vector<size_t> column_positions;
    std::vector<std::function<Int64(const BlockInfo &)>> virtual_time_columns_calc;
    size_t total_physical_columns_in_schema = 0;
    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    // FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};

using StreamingStoreSourceChannelPtr = std::shared_ptr<StreamingStoreSourceChannel>;
using StreamingStoreSourceChannelWeakPtr = std::weak_ptr<StreamingStoreSourceChannel>;
}
