#pragma once

#include <NativeLog/Record/Record.h>
#include <Storages/Streaming/StreamingStoreSourceBase.h>
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

    void attachTo(std::shared_ptr<StreamingStoreSourceMultiplexer> new_multiplexer);

    void recover(CheckpointContextPtr ckpt_ctx_) override;

    String getName() const override { return "StreamingStoreSourceChannel"; }

    UInt32 getID() const { return id; }

    void add(nlog::RecordPtrs records);

private:
    void readAndProcess() override;
    std::pair<String, Int32> getStreamShard() const override;

private:
    static std::atomic<uint32_t> sequence_id;

    UInt32 id;

    mutable std::mutex multiplexer_mutex;
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer;

    Int32 record_consume_timeout_ms = 100;

    /// FIXME, use another lock-free one?
    ConcurrentBoundedQueue<nlog::RecordPtrs> records_queue;
};

using StreamingStoreSourceChannelPtr = std::shared_ptr<StreamingStoreSourceChannel>;
using StreamingStoreSourceChannelWeakPtr = std::weak_ptr<StreamingStoreSourceChannel>;
}
