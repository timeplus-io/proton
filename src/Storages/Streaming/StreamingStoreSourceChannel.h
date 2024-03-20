#pragma once

#include "StreamingStoreSourceBase.h"

#include <NativeLog/Record/Record.h>
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
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr query_context_,
        Poco::Logger * log_);

    ~StreamingStoreSourceChannel() override;

    String getName() const override { return "StreamingStoreSourceChannel"; }

    String description() const override;

    UInt32 getID() const { return id; }

    void add(nlog::RecordPtrs records);

private:
    void readAndProcess() override;
    std::pair<String, Int32> getStreamShard() const override;

private:
    static std::atomic<uint32_t> sequence_id;

    UInt32 id;
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer;

    /// FIXME, use another lock-free one?
    ConcurrentBoundedQueue<nlog::RecordPtrs> records_queue;
};

using StreamingStoreSourceChannelPtr = std::shared_ptr<StreamingStoreSourceChannel>;
using StreamingStoreSourceChannelWeakPtr = std::weak_ptr<StreamingStoreSourceChannel>;
}
