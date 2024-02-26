#pragma once

#include "IngestingBlocks.h"

#include <base/shared_ptr_helper.h>
#include <pcg_random.hpp>

#include <KafkaLog/Results.h>
#include <NativeLog/Record/Record.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>

namespace nlog
{
class NativeLog;
}

namespace DB
{
class StreamShard;
struct KafkaLogContext;

/** A StorageStream is an table engine that uses merge tree and replicated via
  * distributed write ahead log which is now implemented by using Kafka. Users can issue
  * distributed data ingestion and distributed queries against this single table engine directly.
  * The goals of this table engine are resolving the following major requirements
  *   1. Large scale perf data ingestion
  *   2. Streaming query
  *   3. Simplified usability (from end users point of view)
  */
class StorageStream final : public shared_ptr_helper<StorageStream>, public MergeTreeData
{
    friend struct shared_ptr_helper<StorageStream>;

public:
    void startup() override;
    void shutdown() override;
    ~StorageStream() override;

    String getName() const override;

    bool isRemote() const override;

    bool supportsParallelInsert() const override;

    bool supportsIndexForIn() const override;

    bool supportsSubcolumns() const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    NamesAndTypesList getVirtualsHistory() const;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr context) override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void preDrop() override;

    void drop() override;

    void preRename(const StorageID & new_table_id) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    void checkTableCanBeDropped(ContextPtr context) const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo &) const override;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    bool prefersLargeBlocks() const override { return false; }

private:
    /// Partition helpers

    /// Tries to drop part in background without any waits or throwing exceptions in case of errors.
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;

    PartitionCommandsResultInfo
    attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) override;

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;

    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overridden by children, because they can have different
    /// mechanisms for parts locking
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    /// Return most recent mutations commands for part which weren't applied
    /// Used to receive AlterConversions for part and apply them on fly. This
    /// method has different implementations for replicated and non replicated
    /// MergeTree because they store mutations in different way.
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    void startBackgroundMovesIfNeeded() override;

    /// Returns default settings for storage with possible changes from global config.
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    /// Distributed query
    QueryProcessingStage::Enum getQueryProcessingStageRemote(
        ContextPtr context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const;

    ClusterPtr getOptimizedCluster(ContextPtr context, const StorageSnapshotPtr & storage_snapshot, const ASTPtr & query_ptr) const;

    ClusterPtr getCluster() const;

    ClusterPtr
    skipUnusedShards(ClusterPtr cluster, const ASTPtr & query_ptr, const StorageSnapshotPtr & storage_snapshot, ContextPtr context) const;

    void readRemote(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage);

public:
    enum class QueryMode : uint8_t
    {
        STREAMING, /// streaming query
        STREAMING_CONCAT, /// streaming query with backfilled historical data
        HISTORICAL /// historical query
    };

    using StreamShardPtrs = std::vector<std::shared_ptr<StreamShard>>;
    struct ShardsToRead
    {
        QueryMode mode;
        StreamShardPtrs local_shards; /// Also as streaming shards when mode is STREAMING || STREAMING_CONCAT
        StreamShardPtrs remote_shards;

        bool requireDistributed() const { return !remote_shards.empty(); }
    };

private:
    ShardsToRead getRequiredShardsToRead(ContextPtr context_, const SelectQueryInfo & query_info) const;

    void readChangelog(
        const ShardsToRead & shards_to_read,
        QueryPlan & query_plan,
        Names column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    void readStreaming(
        const StreamShardPtrs & shards_to_read,
        QueryPlan & query_plan,
        SelectQueryInfo & query_info,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr context_);

    void readConcat(
        const StreamShardPtrs & shards_to_read,
        QueryPlan & query_plan,
        SelectQueryInfo & query_info,
        Names column_names,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    void readHistory(
        const StreamShardPtrs & shards_to_read,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

public:
    IColumn::Selector createSelector(const ColumnWithTypeAndName & result) const;
    IColumn::Selector createSelector(const Block & block) const;

    const ExpressionActionsPtr & getShardingKeyExpr() const;

    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }

    Int32 getShards() const { return shards; }
    Int32 getReplicationFactor() const { return replication_factor; }

    size_t getRandomShardIndex() const;
    Int32 getNextShardIndex() const;

    void getIngestionStatuses(const std::vector<UInt64> & block_ids, std::vector<IngestingBlocks::IngestStatus> & statuses) const;

    UInt64 nextBlockId() const;

    IngestMode ingestMode() const;

    /// return true, if stream is in maintenance mode
    bool isMaintain() const;

    void reInit();

    /// Return (shard_id, committed_sn) pairs
    std::vector<std::pair<Int32, Int64>> lastCommittedSequences() const;

    const StreamShardPtrs & getStreamShards() const { return stream_shards; }

    /// Used for validity check before background ingestion of materialized view
    /// @native_log or @kafka_log become not nullptr only if StorageStream::startup completes.
    /// Need to ensure thread safety, since `isReady()` can be called by other threads
    bool isReady() const override { return log_initialized.test(); }

    bool isInmemory() const { return getSettings()->storage_type.value == "memory"; }

    std::vector<nlog::RecordSN> getLastSNs() const;

    bool supportsStreamingQuery() const override { return true; }

    friend class StreamSink;
    friend class MergeTreeData;

protected:
    StorageStream(
        Int32 replication_factor_,
        Int32 shards_,
        const ASTPtr & sharding_key_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergingParams & merging_params_,
        std::unique_ptr<StreamSettings> settings_,
        bool has_force_restore_data_flag_);

private:
    std::vector<Int64> getOffsets(const String & seek_to) const;

private:
    /// FIXME, move ingest APIs to StreamShard
    void append(
        nlog::RecordPtr & record,
        IngestMode ingest_mode,
        klog::AppendCallback callback,
        klog::CallbackData data,
        UInt64 base_block_id,
        UInt64 sub_block_id);

    void
    appendToNativeLog(nlog::RecordPtr & record, IngestMode /*ingest_mode*/, klog::AppendCallback callback, klog::CallbackData data);

    void appendToKafka(
        nlog::RecordPtr & record,
        IngestMode ingest_mode,
        klog::AppendCallback callback,
        klog::CallbackData data,
        UInt64 base_block_id,
        UInt64 sub_block_id);

    void poll(Int32 timeout_ms);

    void cacheVirtualColumnNamesAndTypes();

    void updateLogStoreCodec(const String & settings_codec);

    void checkReady() const;

private:
    Int32 replication_factor;
    Int32 shards;
    ExpressionActionsPtr sharding_key_expr;
    bool rand_sharding_key = false;

    /// Ascending order based on shard id
    StreamShardPtrs stream_shards; /// All shards
    StreamShardPtrs local_shards; /// Only local shards
    StreamShardPtrs remote_shards; /// Only remote virtual shards

    /// For sharding
    bool sharding_key_is_deterministic = false;
    std::vector<UInt64> slot_to_shard;
    String sharding_key_column_name;

    NamesAndTypesList virtual_column_names_and_types;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    mutable pcg64 rng;

    /// For ingest
    mutable std::atomic_uint_fast32_t next_shard = 0;

    /// Outstanding async ingest records
    UInt64 max_outstanding_blocks;
    std::atomic_uint_fast64_t outstanding_blocks = 0;

    nlog::NativeLog * native_log = nullptr;
    KafkaLogContext * kafka_log = nullptr;
    std::atomic_flag log_initialized;

    CompressionMethodByte logstore_codec = CompressionMethodByte::NONE;

    std::atomic_flag inited;
    std::atomic_flag stopped;
};
}
