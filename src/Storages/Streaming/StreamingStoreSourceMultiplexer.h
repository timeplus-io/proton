#pragma once

#include <Storages/Streaming/StreamingBlockReaderKafka.h>
#include <Storages/Streaming/StreamingBlockReaderNativeLog.h>
#include <Storages/Streaming/StreamingStoreSourceChannel.h>

namespace DB
{
class StreamShard;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// The multiplexer fans out one streaming store reader to different streaming queries. This has
/// efficiency of disk read / TFF deserialization, memory allocation etc. But we may introduce
/// new problems like one slow query processing pipeline will slow down other pipelines. So it
/// maintains some metrics for `channel` and detach the channel and its the pipeline when slowness
/// is detected. In future, we can apply more sophistic strategies to balance the resource efficiency
/// and the whole multiplexer's latency / throughput.
///                                                                -> StreamingStoreSourceChannel -> QueryProcessing Pipeline
///    StreamingStore Partition -> StreamingStoreSourceMultiplexer -> StreamingStoreSourceChannel -> QueryProcessing Pipeline
///                                                                -> StreamingStoreSourceChannel -> QueryProcessing Pipeline
/// Multiplexer is multiple-thread safe
/// A multiplexer is bound to a shard
class StreamingStoreSourceMultiplexer final : public std::enable_shared_from_this<StreamingStoreSourceMultiplexer>
{
public:
    using AttachToSharedGroupFunc = std::function<void(std::shared_ptr<StreamingStoreSourceMultiplexer>)>;
    StreamingStoreSourceMultiplexer(
        UInt32 id_,
        std::shared_ptr<StreamShard> storage_,
        ContextPtr global_context,
        Poco::Logger * log_,
        AttachToSharedGroupFunc attach_to_shared_group_func = {});
    ~StreamingStoreSourceMultiplexer();

    StreamingStoreSourceChannelPtr
    createChannel(const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context);

    bool tryDetachChannelsInto(std::shared_ptr<StreamingStoreSourceMultiplexer> new_multiplexer);

    void removeChannel(UInt32 channel_id);

    size_t totalChannels() const;

    bool isShutdown() const { return shutdown; }

    std::pair<String, Int32> getStreamShard() const;

    /// NOTE: Reset sequence number only before startup()
    void resetSequenceNumber(Int64 start_sn);
    void startup();

private:
    inline nlog::RecordPtrs read();
    void backgroundPoll();
    void fanOut(nlog::RecordPtrs records);
    void doShutdown();

    friend class StreamingStoreSourceMultiplexers;

private:
    UInt32 id;
    std::shared_ptr<StreamShard> stream_shard;
    std::unique_ptr<StreamingBlockReaderKafka> kafka_reader;
    std::unique_ptr<StreamingBlockReaderNativeLog> nativelog_reader;

    std::atomic_flag started;

    AttachToSharedGroupFunc attach_to_shared_group_func;
    Int64 fanout_sn = -1;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;

    std::unique_ptr<ThreadPool> poller;
    std::atomic<bool> shutdown = false;

    mutable std::mutex channels_mutex;
    std::unordered_map<UInt32, StreamingStoreSourceChannelWeakPtr> channels;

    struct FanOutMetrics
    {
        Int64 total_time = 0;
        Int64 total_count = 0;
    };

    FanOutMetrics metrics;
    Int64 last_metrics_log_time;

    Poco::Logger * log;
};

using StreamingStoreSourceMultiplexerPtr = std::shared_ptr<StreamingStoreSourceMultiplexer>;
using StreamingStoreSourceMultiplexerPtrs = std::list<StreamingStoreSourceMultiplexerPtr>;

/// A multiplexers is bound to a stream
class StreamingStoreSourceMultiplexers final
{
public:
    StreamingStoreSourceMultiplexers(ContextPtr global_context_, Poco::Logger * log_);

    StreamingStoreSourceChannelPtr createChannel(
        std::shared_ptr<StreamShard> stream_shard,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr query_context,
        Int64 start_sn);

private:
    StreamingStoreSourceChannelPtr createIndependentChannelForRecover(
        std::shared_ptr<StreamShard> stream_shard,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr query_context);

    StreamingStoreSourceChannelPtr createIndependentChannelWithSeekTo(
        std::shared_ptr<StreamShard> stream_shard,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr query_context,
        Int64 start_sn);

    void attachToSharedGroup(StreamingStoreSourceMultiplexerPtr multiplexer);

    uint32_t getMultiplexerID() { return multiplexer_id.fetch_add(1); }

private:
    ContextPtr global_context;
    Poco::Logger * log;

    static std::atomic<uint32_t> multiplexer_id;

    std::mutex multiplexers_mutex;
    std::unordered_map<Int32, StreamingStoreSourceMultiplexerPtrs> multiplexers;
    StreamingStoreSourceMultiplexerPtrs detached_multiplexers;
};
}
