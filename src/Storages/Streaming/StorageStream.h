#pragma once

#include "IngestingBlocks.h"
#include "StreamCallbackData.h"
#include "StreamingStoreSourceMultiplexer.h"

#include <base/ClockUtils.h>
#include <base/shared_ptr_helper.h>
#include <pcg_random.hpp>

#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <KafkaLog/KafkaWAL.h>
#include <KafkaLog/KafkaWALConsumerMultiplexer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Common/ProtonCommon.h>
#include <Common/ThreadPool.h>


namespace DB
{
class StorageMergeTree;

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

    /// Here we inverted the implementation : Pipe read() -> void read
    /// which shall be the other way. Ref : IStorage default implementation
    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void readStreaming(
        QueryPlan & query_plan,
        SelectQueryInfo & query_info,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context_);

    void readHistory(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams);

    void readConcat(
        QueryPlan & query_plan,
        SelectQueryInfo & query_info,
        Names column_names,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size);

    NamesAndTypesList getVirtuals() const override;

    NamesAndTypesList getVirtualsHistory() const;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

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

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr, QueryProcessingStage::Enum to_stage, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo &) const override;

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
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info) const;

    ClusterPtr getOptimizedCluster(ContextPtr context, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const;

    ClusterPtr getCluster() const;

    ClusterPtr
    skipUnusedShards(ClusterPtr cluster, const ASTPtr & query_ptr, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) const;

    void readRemote(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage);

public:
    IColumn::Selector createSelector(const ColumnWithTypeAndName & result) const;
    IColumn::Selector createSelector(const Block & block) const;

    const ExpressionActionsPtr & getShardingKeyExpr() const;

    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }

    Int32 getShards() const { return shards; }
    Int32 getReplicationFactor() const { return replication_factor; }

    size_t getRandomShardIndex();
    size_t getNextShardIndex() const;

    Int32 currentShard() const { return shard; }
    void getIngestionStatuses(const std::vector<UInt64> & block_ids, std::vector<IngestingBlocks::IngestStatus> & statuses) const
    {
        if (kafka)
            kafka->ingesting_blocks.getStatuses(block_ids, statuses);
    }

    UInt64 nextBlockId() const
    {
        if (kafka)
            return kafka->ingesting_blocks.nextId();

        return 0;
    }

    String logstoreType() const
    {
        if (kafka)
            return ProtonConsts::LOGSTORE_KAFKA;

        return ProtonConsts::LOGSTORE_NATIVE_LOG;
    }

    bool isLogstoreKafka() const { return kafka != nullptr; }

    nlog::RecordSN lastSN() const;

    String streamingStorageClusterId() const;

    friend struct StreamCallbackData;
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
    void initLog();
    void initKafkaLog();
    void initNativeLog();
    void deinitNativeLog();
    bool requireDistributedQuery(ContextPtr context_) const;
    std::vector<Int64> getOffsets(const String & seek_to) const;

private:
    struct WriteCallbackData
    {
        UInt64 block_id;
        UInt64 sub_block_id;

        StorageStream * storage;

        WriteCallbackData(UInt64 block_id_, UInt64 sub_block_id_, StorageStream * storage_)
            : block_id(block_id_), sub_block_id(sub_block_id_), storage(storage_)
        {
            ++storage_->outstanding_blocks;
        }

        ~WriteCallbackData() { --storage->outstanding_blocks; }
    };

    void appendAsync(nlog::Record & record, UInt64 block_id, UInt64 sub_block_id);

    void writeCallback(const klog::AppendResult & result, UInt64 block_id, UInt64 sub_block_id);

    static void writeCallback(const klog::AppendResult & result, void * data);

    nlog::RecordSN snLoaded() const;
    void mergeBlocks(Block & lhs, Block & rhs);
    bool dedupBlock(const nlog::RecordPtr & record);
    void addIdempotentKey(const String & key);
    void buildIdempotentKeysIndex(const std::deque<std::shared_ptr<String>> & idempotent_keys_);

    void commit(nlog::RecordPtrs records, SequenceRanges missing_sequence_ranges);

    using SequencePair = std::pair<nlog::RecordSN, nlog::RecordSN>;

    void doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges);
    void commitSN();
    void commitSNLocal(nlog::RecordSN commit_sn);
    void commitSNRemote(nlog::RecordSN commit_sn);

    void finalCommit();
    void periodicallyCommit();

    void progressSequences(const SequencePair & seq);
    void progressSequencesWithoutLock(const SequencePair & seq);
    Int64 maxCommittedSN() const;

    static void consumeCallback(nlog::RecordPtrs records, klog::ConsumeCallbackData * data);

    /// Dedicate thread consumption
    void backgroundPollKafka();

    /// Shared mode consumption
    void addSubscriptionKafka();
    void removeSubscriptionKafka();

    void backgroundPollNativeLog();
    void cacheVirtualColumnNamesAndTypes();

    std::vector<Int64> sequencesForTimestamps(Int64 ts, bool append_time = false) const;

private:
    Int32 replication_factor;
    Int32 shards;
    ExpressionActionsPtr sharding_key_expr;
    bool rand_sharding_key = false;

    /// Current shard. DWAL partition and table shard is 1:1 mapped
    Int32 shard = -1;

    IngestMode default_ingest_mode;

    /// For sharding
    bool sharding_key_is_deterministic = false;
    std::vector<UInt64> slot_to_shard;
    String sharding_key_column_name;

    NamesAndTypesList virtual_column_names_and_types;

    /// Cached ctx for reuse
    struct KafkaLog
    {
        KafkaLog(const String & topic_, Int32 shards_, Int32 replication_factor_, Int64 timeout_ms_, Poco::Logger * log_)
            : append_ctx(topic_, shards_, replication_factor_)
            , consume_ctx(topic_, shards_, replication_factor_)
            , ingesting_blocks(timeout_ms_, log_)
        {
        }

        ~KafkaLog() { shutdown(); }

        void shutdown()
        {
            if (log)
                log.reset();
        }

        String topic() const { return append_ctx.topic; }

        klog::KafkaWALContext append_ctx;
        klog::KafkaWALContext consume_ctx;

        /// For Produce and dedicated consumption
        klog::KafkaWALPtr log;

        /// For shared consumption
        klog::KafkaWALConsumerMultiplexerPtr multiplexer;
        std::weak_ptr<klog::KafkaWALConsumerMultiplexer::CallbackContext> shared_subscription_ctx;

        IngestingBlocks ingesting_blocks;
    };

    std::unique_ptr<KafkaLog> kafka;

    /// Local checkpoint threshold timer
    Int64 last_commit_ts = MonotonicSeconds::now();

    /// Forwarding storage if it is not virtual
    std::shared_ptr<StorageMergeTree> storage;
    std::optional<ThreadPool> poller;

    ThreadPool & part_commit_pool;

    mutable std::mutex sns_mutex;
    nlog::RecordSN last_sn = -1; /// To be committed to DWAL
    nlog::RecordSN prev_sn = -1; /// Committed to DWAL
    nlog::RecordSN local_sn = -1; /// Committed to `committed_sn.txt`
    std::set<SequencePair> local_committed_sns; /// Committed to `Part` folder
    std::deque<SequencePair> outstanding_sns;

    /// Idempotent keys caching
    std::deque<std::shared_ptr<String>> idempotent_keys;
    std::unordered_set<StringRef, StringRefHash> idempotent_keys_index;

    std::unique_ptr<StreamCallbackData> callback_data;

    std::unique_ptr<StreamingStoreSourceMultiplexers> source_multiplexers;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;

    mutable std::atomic_uint_fast64_t next_shard = 0;

    /// Outstanding async ingest records
    UInt64 max_outstanding_blocks;
    std::atomic_uint_fast64_t outstanding_blocks = 0;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
};
}
