#pragma once

#include <Storages/Stream/StreamingBlockReaderBase.h>
#include <Storages/Stream/StreamingStoreSourceChannel.h>

#include <Storages/StorageSnapshot.h>

namespace DB
{
class StreamShardStore;
using StreamShardStorePtr = std::shared_ptr<StreamShardStore>;

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
    StreamingStoreSourceMultiplexer(UInt32 id_, StreamShardStorePtr storage_, ContextPtr global_context, LoggerPtr log_);
    ~StreamingStoreSourceMultiplexer();

    StreamingStoreSourceChannelPtr
    createChannel(const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context);

    void removeChannel(UInt32 channel_id);

    size_t totalChannels() const;

    bool isShutdown() const { return shutdown; }

    std::pair<String, Int32> getStreamShard() const;

    const cluster::StreamShard & streamShard() const;

private:
    void backgroundPoll();
    void fanOut(cluster::SchemaRecordPtrs records);
    void doShutdown();

private:
    UInt32 id;
    StreamShardStorePtr stream_shard_store;
    // StreamingBlockReaderPtr reader;

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

    LoggerPtr logger;
};

using StreamingStoreSourceMultiplexerPtr = std::shared_ptr<StreamingStoreSourceMultiplexer>;
using StreamingStoreSourceMultiplexerPtrs = std::list<StreamingStoreSourceMultiplexerPtr>;

/// A multiplexers is bound to a stream
class StreamingStoreSourceMultiplexers final
{
public:
    StreamingStoreSourceMultiplexers(StreamShardStorePtr stream_shard_store_, ContextPtr global_context_, LoggerPtr log_);

    StreamingStoreSourceChannelPtr
    createChannel(Int32 shard, const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context);

private:
    std::shared_ptr<StreamShardStore> stream_shard_store;
    ContextPtr global_context;
    LoggerPtr logger;

    std::mutex multiplexers_mutex;
    std::unordered_map<Int32, StreamingStoreSourceMultiplexerPtrs> multiplexers;
};
}
