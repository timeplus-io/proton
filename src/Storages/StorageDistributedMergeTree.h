#pragma once

#include <pcg_random.hpp>
#include <ext/shared_ptr_helper.h>

#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/IngestingBlocks.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Common/ThreadPool.h>


namespace DB
{
class StorageMergeTree;

/** A StorageDistributedMergeTree is an table engine that uses merge tree and replicated via
  * distributed write ahead log which is now implemented by using Kafka. Users can issue
  * distributed data ingestions and distributed queries against this single table engine directly.
  * The goals of this table engine are resolving the following major requirements
  *   1. Large scale perf data ingestion
  *   2. Streaming query
  *   3. Simplified usabilities (from end users point of view)
  */
class StorageDistributedMergeTree final : public ext::shared_ptr_helper<StorageDistributedMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageDistributedMergeTree>;

public:
    void startup() override;
    void shutdown() override;
    ~StorageDistributedMergeTree() override = default;

    String getName() const override;

    bool isRemote() const override;

    bool supportsParallelInsert() const override;

    bool supportsIndexForIn() const override;

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

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    NamesAndTypesList getVirtuals() const override;

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

    void drop() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void alter(const AlterCommands & commands, ContextPtr context, TableLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    std::optional<JobAndPool> getDataProcessingJob() override;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum to_stage, SelectQueryInfo &) const override;

private:
    /// Partition helpers

    void dropPartition(const ASTPtr & partition, bool detach, bool drop_part, ContextPtr context, bool throw_if_noop = true) override;

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

    /// Distributed query
    QueryProcessingStage::Enum
    getQueryProcessingStageRemote(ContextPtr context, QueryProcessingStage::Enum to_stage, SelectQueryInfo & query_info) const;

    ClusterPtr getOptimizedCluster(ContextPtr context, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const;

    ClusterPtr getCluster() const;

    ClusterPtr
    skipUnusedShards(ClusterPtr cluster, const ASTPtr & query_ptr, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) const;

    void readRemote(QueryPlan & query_plan, SelectQueryInfo & query_info, ContextPtr context, QueryProcessingStage::Enum processed_stage);

public:
    IColumn::Selector createSelector(const ColumnWithTypeAndName & result) const;
    IColumn::Selector createSelector(const Block & block) const;

    const ExpressionActionsPtr & getShardingKeyExpr() const;

    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }

    Int32 getShards() const { return shards; }
    Int32 getReplicationFactor() const { return replication_factor; }

    size_t getRandomShardIndex();
    Int32 currentShard() const { return shard; }
    void getIngestionStatuses(const std::vector<String> & poll_ids, std::vector<IngestingBlocks::IngestStatus> & statuses) const
    {
        ingesting_blocks.getStatuses(poll_ids, statuses);
    }

    IDistributedWriteAheadLog::RecordSequenceNumber lastSequenceNumber() const;

    friend class DistributedMergeTreeBlockOutputStream;
    friend class MergeTreeData;

protected:
    StorageDistributedMergeTree(
        Int32 replication_factor_,
        Int32 shards_,
        const ASTPtr & sharding_key_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach_,
        ContextPtr context_,
        const String & date_column_name_,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag_);

private:
    void initWal();

private:
    struct WriteCallbackData
    {
        String query_status_poll_id;
        UInt16 block_id;
        StorageDistributedMergeTree * storage;

        WriteCallbackData(const String & query_status_poll_id_, UInt16 block_id_, StorageDistributedMergeTree * storage_)
            : query_status_poll_id(query_status_poll_id_), block_id(block_id_), storage(storage_)
        {
        }
    };

    WriteCallbackData * writeCallbackData(const String & query_status_poll_id, UInt16 block_id);
    void writeCallback(const IDistributedWriteAheadLog::AppendResult & result, const String & query_status_poll_id, UInt16 block_id);

    static void writeCallback(const IDistributedWriteAheadLog::AppendResult & result, void * data);

    IDistributedWriteAheadLog::RecordSequenceNumber sequenceNumberLoaded() const;
    void backgroundConsumer();
    void mergeBlocks(Block & lhs, Block & rhs);
    bool dedupBlock(const IDistributedWriteAheadLog::RecordPtr & record);
    void addIdempotentKey(const String & key);
    void buildIdempotentKeysIndex(const std::deque<std::shared_ptr<String>> & idempotent_keys_);

    void commit(const IDistributedWriteAheadLog::RecordPtrs & records, std::any & dwal_consume_ctx);

    using SequencePair = std::pair<IDistributedWriteAheadLog::RecordSequenceNumber, IDistributedWriteAheadLog::RecordSequenceNumber>;

    void doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, std::any & dwal_consume_ctx);
    void commitSN(std::any & dwal_consume_ctx);
    void commitSNLocal(IDistributedWriteAheadLog::RecordSequenceNumber commit_sn);
    void commitSNRemote(IDistributedWriteAheadLog::RecordSequenceNumber commit_sn, std::any & dwal_consume_ctx);
    void progressSequences(const SequencePair & seq);
    void progressSequencesWithLock(const SequencePair & seq);

private:
    Int32 replication_factor;
    Int32 shards;
    ExpressionActionsPtr sharding_key_expr;

    /// Current shard. DWAL partition and table shard is 1:1 mapped
    Int32 shard = -1;

    /// For sharding
    bool sharding_key_is_deterministic = false;
    std::vector<UInt64> slot_to_shard;
    String sharding_key_column_name;

    /// Cached ctx for reuse
    std::any dwal_append_ctx;

    DistributedWriteAheadLogPtr dwal;
    IngestingBlocks & ingesting_blocks;

    /// Local checkpoint threshold timer
    std::chrono::time_point<std::chrono::steady_clock> last_commit_ts = std::chrono::steady_clock::now();

    /// Forwarding storage if it is not virtual
    std::shared_ptr<StorageMergeTree> storage;
    std::optional<ThreadPool> tailer;

    ThreadPool & part_commit_pool;

    mutable std::mutex sns_mutex;
    IDistributedWriteAheadLog::RecordSequenceNumber last_sn = -1; /// To be committed to DWAL
    IDistributedWriteAheadLog::RecordSequenceNumber prev_sn = -1; /// Committed to DWAL
    IDistributedWriteAheadLog::RecordSequenceNumber local_sn = -1; /// Committed to `committed_sn.txt`
    std::set<SequencePair> local_committed_sns; /// Committed to `Part` folder
    std::deque<SequencePair> outstanding_sns;

    /// Idempotent keys caching
    std::deque<std::shared_ptr<String>> idempotent_keys;
    std::unordered_set<StringRef, StringRefHash> idempotent_keys_index;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
};
}
