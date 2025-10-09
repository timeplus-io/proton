#pragma once

#include <Storages/Stream/IngestMode.h>
#include <Storages/Stream/IngestingBlocks.h>
#include <Storages/Stream/StreamCallbackData.h>
#include <Storages/Stream/StreamingStoreSourceMultiplexer.h>

#include <Cluster/Common/AckSemantic.h>
#include <Cluster/Common/Error.h>
#include <Cluster/Common/StreamShard.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/Stream/InMemoryIdempotentKeys.h>
#include <base/ClockUtils.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}


namespace DB
{
class StorageMergeTree;
class StorageStream;

/// StreamShardStore contains 2 parts
/// 1. one shard for streaming log store
/// 2. one shard for historical store (optional).
/// A background process consumes data from log store shard and then commits the data
/// to the shard of historical store. Please note that the historical store shard is optional.
class StreamShardStore final : public std::enable_shared_from_this<StreamShardStore>
{
public:
    StreamShardStore(
        UInt32 shard_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeData::MergingParams & merging_params_,
        std::unique_ptr<StreamSettings> settings_,
        bool has_force_restore_data_flag_,
        bool inmemory_,
        StorageStream & storage_stream_);

    ~StreamShardStore();

    void startup();
    void shutdown(bool dropping);

    void readStreaming(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        size_t max_block_size,
        size_t shard_num_streams);

    /// \return maximum sequence number of current snapshot data, `< 0` if no data
    Int64 readHistorical(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    void readConcat(
        QueryPlan & query_plan,
        Names column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams,
        size_t streaming_num_streams);

    cluster::CallResultV<int64_t> append(cluster::ByteVector && data, int64_t max_event_time, cluster::AckSemantic ack, int64_t timeout_ms);

    bool isStopped() const noexcept { return stopped.test(std::memory_order_relaxed); }

    int64_t lastSN() const;

    String logStoreClusterId() const;

    StorageStream & storageStream() { return storage_stream; }

    const StorageMergeTree * getStorage() const { return storage ? storage.get() : nullptr; }

    IngestMode ingestMode() const { return default_ingest_mode; }

    bool isNativeLog() const { return true; } // always use NativeLog

    bool isLocal() const noexcept;

    UInt64 getLogStoreDiskSize() const;

    UInt64 getStorageSize() const;

    const auto & streamShard() const noexcept { return stream_shard; }

    auto shard() const noexcept { return stream_shard.shard(); }

    const auto & streamID() const noexcept { return stream_shard.id(); }

    const auto & streamIDShard() const noexcept { return stream_shard.id_shard; }

    std::pair<String, Int32> getStreamShard() const { return {toString(stream_shard.id()), stream_shard.shard()}; }

    bool isInmemory() const noexcept { return inmemory; }

    // always local/real replica
    bool isVirtualReplica() const noexcept { return false; }

    UInt64 maxEntrySize() const;

    void renameInMemory(const StorageID & new_table_id)
    {
        assert(stream_shard.id() == new_table_id.uuid);
        assert(stream_shard.ns == new_table_id.database_name);
        assert(stream_shard.name != new_table_id.table_name);
        stream_shard.name = new_table_id.table_name;
    }

    /// Remove historical store and log store from file system
    /// if there are backing storages for them
    void drop();

    /// Alter settings
    /// AlterLockHolder => std::unique_lock which is defined in IStorage
    /// void alter(const AlterCommands & commands, ContextPtr context_, std::unique_lock<std::timed_mutex> & alter_lock_holder);

    InMemoryIdempotentKeysPtr getIdempotentKeysSnapshot(Int64 max_sn, size_t max_ids) const
    {
        return idempotent_keys->snapshotTo(max_sn, max_ids);
    }

    std::optional<Int64> committedSequence() const;

    /// \return last sequence number which gets applied to historical store
    std::optional<Int64> appliedSequence() const;

    std::pair<Int64, Int64> sequenceRange() const;

private:
    void initLog();
    void initNativeLog();

    int64_t snLoaded() const;

    void backgroundPollNativeLog();

    static void mergeBlocks(Block & lhs, Block & rhs);

    bool dedupBlock(const cluster::SchemaRecordPtr & record);

    using SequencePair = std::pair<int64_t, int64_t>;

    void commit(cluster::SchemaRecordPtrs records, SequenceRanges missing_sequence_ranges);
    void doCommit(
        Block block,
        SequencePair seq_pair,
        std::shared_ptr<IdempotentKeys> keys,
        SequenceRanges missing_sequence_ranges,
        StorageMetadataPtr metadata);
    void commitSN();
    void commitSNLocal(int64_t commit_sn);
    void commitSNRemote(int64_t commit_sn);

    void finalCommit();
    void periodicallyCommit();

    void progressSequences(const SequencePair & seq);
    void progressSequencesWithLockHeld(const SequencePair & seq);
    Int64 maxCommittedSN() const;

    std::vector<Int64> sequencesForTimestamps(std::vector<Int64> timestamps, bool append_time = false) const;

    Int64 getOffset(const SeekToInfoPtr & seek_to_info) const;

    static void consumeCallback(cluster::SchemaRecordPtrs records, cluster::klog::ConsumeCallbackData * data);

    void alterLogSettings(
        const std::unordered_map<std::string, int32_t> & flush_settings,
        const std::unordered_map<std::string, int64_t> & retention_settings);

    void handleControlCommand(cluster::protocol::OpCode opcode, const Block & control_block, Int64 sn);
    void truncate(std::string_view truncate_partition_command);
    void deleteRows(std::string_view delete_rows_command);
    void alterPartition(std::string_view alter_partition_command);
    void rebuildIndex(std::string_view rebuild_index_command);
    void dropIndex(std::string_view drop_index_command);
    void optimize(Int64 sn, const Block & control_block);

    friend struct StreamCallbackData;
    friend class StorageStream;

private:
    std::atomic_flag started;
    std::atomic_flag stopped;
    std::atomic<bool> is_dropping = false;

    /// Cache cluster::StreamShard to avoid evaluation everytime
    /// when talking to NativeLog
    cluster::StreamShard stream_shard;

    // no replica_set or current_replica needed

    bool inmemory;

    StorageStream & storage_stream;

    IngestMode default_ingest_mode{};

    /// Local checkpoint threshold timer
    Int64 last_commit_ts = MonotonicSeconds::now();

    /// Forwarding storage for historical data if it is not virtual
    std::shared_ptr<StorageMergeTree> storage;
    std::optional<ThreadPool> poller;

    ThreadPool & part_commit_pool;

    mutable std::mutex sns_mutex;
    int64_t last_sn = -1; /// To be committed to logstore
    int64_t prev_sn = -1; /// Committed to logstore
    int64_t local_sn = -1; /// Committed to `committed_sn.txt`
    std::set<SequencePair> local_committed_sns; /// Committed to `Part` folder
    std::deque<SequencePair> outstanding_sns;

    /// Idempotent keys caching
    InMemoryIdempotentKeysPtr idempotent_keys;

    std::unique_ptr<CachedSchemaProvider> schema_provider;
    std::unique_ptr<StreamCallbackData> callback_data;

    std::unique_ptr<StreamingStoreSourceMultiplexers> source_multiplexers;

    LoggerPtr logger;
};


}
