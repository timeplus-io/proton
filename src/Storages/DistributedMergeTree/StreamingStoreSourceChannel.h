#pragma once

#include "StreamingStoreSourceBase.h"

#include <DistributedWALClient/Record.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{
class StreamingStoreSourceMultiplexer;

class StreamingStoreSourceChannel final : public StreamingStoreSourceBase
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

    void add(DWAL::RecordPtrs records);

private:
    void readAndProcess() override;

private:
    static std::atomic<uint32_t> sequence_id;

    UInt32 id;
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer;

    /// FIXME, use another lock-free one?
    ConcurrentBoundedQueue<DWAL::RecordPtrs> records_queue;
};

using StreamingStoreSourceChannelPtr = std::shared_ptr<StreamingStoreSourceChannel>;
using StreamingStoreSourceChannelWeakPtr = std::weak_ptr<StreamingStoreSourceChannel>;
}
