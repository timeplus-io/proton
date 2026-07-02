#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/QueryShard.h>
#include <Storages/ShardAppliedSequence.h>
#include <Storages/ShardCommittedSequence.h>
#include <Storages/Stream/IngestMode.h>
#include <Storages/Stream/IngestingBlocks.h>
#include <Storages/StreamIOStats.h>

#include <base/shared_ptr_helper.h>
#include <pcg_random.hpp>

#include <Cluster/KafkaLog/Results.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

namespace cluster::client
{
class Client;
}

namespace DB
{
class StreamShardStore;
using StreamShardStorePtr = std::shared_ptr<StreamShardStore>;
using StreamShardStorePtrs = std::vector<StreamShardStorePtr>;

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
    ~StorageStream() override;

    void startup() override;
    void shutdown(bool dropping) override;

    String getName() const override;

    bool isRemote() const override { return isVirtualStorage(); }

    bool isLocal() const override { return is_local; }

    bool supportsParallelInsert() const override { return true; }

    bool supportsIndexForIn() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsTTL() const override { return true; }

    bool squashInsert() const noexcept override { return false; }

    bool prefersLargeBlocks() const override { return false; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static NamesAndTypesList getVirtualsOfAppendOnly();

    NamesAndTypesList getVirtuals() const override;

    NamesAndTypesList getVirtualsHistorical() const;

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

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void drop() override;

    void preRename(const StorageID & new_table_id) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    void alterLogSettings(
        const std::unordered_map<std::string, int32_t> & flush_settings,
        const std::unordered_map<std::string, int64_t> & retention_settings);

    void applyAlterCommandsToAllShards(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder) override;

    void checkTableCanBeDropped(ContextPtr context) const override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings) const override;

    Pipe
    alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr query_context) override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo &) const override;

    void updateSettingsAndVersionForInnerStorage(const StorageInMemoryMetadata & new_metadata);

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    bool isVirtualStorage() const override;

    UInt64 getLogStoreDiskSize() const;
    UInt64 getStorageSize() const override;

    /// Metrics-only variant: TTL-cached at the per-shard level. Not for correctness paths.
    UInt64 getHistoricalStorageSizeForMetrics() const;

    Int64 getMetricTime(bool reset = true) override;
    UInt64 readBytes(bool reset = true, bool external_ingress = false) override;
    UInt64 readRows(bool reset = true, bool external_ingress = false) override;
    UInt64 writtenBytes(bool reset = true, bool external_ingress = false) override;
    UInt64 writtenRows(bool reset = true, bool external_ingress = false) override;

    bool supportsLightIngest(const Settings & settings) const noexcept override { return settings.enable_light_ingest.value; }

    /// Return log max entry size, 0 means unlimited.
    UInt64 maxEntrySize() const;

    std::vector<ShardCommittedSequence> committedSequences() const;

    std::vector<ShardAppliedSequence> appliedSequences() const;

    bool hasLightweightDeletedMask() const override;
    bool supportsLightweightDelete() const noexcept override { return true; }

    SnapshotDataWithExpiration readSnapshot(const Names & required_columns, Int64 default_cache_expire_sec) override;

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
    MutationCommands getAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    void startBackgroundMovesIfNeeded() override;

public:
    /// Returns default settings for storage with possible changes from global config.
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

private:
    ClusterPtr getCluster(ContextPtr context) const;

public:
    struct ShardsToRead
    {
        QueryMode mode;
        std::vector<StreamShardStorePtr> shards;
    };

private:
    ShardsToRead
    getShardsToRead(const ContextPtr & local_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const;

    void doRead(
        const ShardsToRead & shards_to_read,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    void doReadChangelog(
        const ShardsToRead & shards_to_read,
        QueryPlan & query_plan,
        Names column_names,
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

    UInt32 getShards() const noexcept { return shards; }
    UInt32 getPhysicalShards() const noexcept;

    size_t getRandomShardIndex() const noexcept;
    UInt32 getNextShardIndex() const noexcept;

    IngestMode ingestMode() const;

    StreamShardStorePtrs getStreamShards() const { return stream_shards; }

    /// Used for validity check before background ingestion of materialized view
    /// @native_log or @kafka_log become not nullptr only if StorageStream::startup completes.
    /// Need to ensure thread safety, since `isReady()` can be called by other threads
    bool isReady() const override { return ready.load(); }

    bool isInmemory() const override { return getSettings()->storage_type.value == "memory"; }

    const String & getStorageType() const { return getSettings()->storage_type.value; }

    const String & getEngineMode() const { return getSettings()->mode; }

    std::vector<int64_t> getLastSNs() const;

    bool supportsStreamingQuery() const override { return true; }

    friend class StreamSink;
    friend class MergeTreeData;
    friend class StreamShardStore;

protected:
    StorageStream(
        UInt32 shards_,
        const ASTPtr & sharding_expr_,
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
    void init();
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

    /// FIXME, move ingest APIs to StreamShardStore
    int append(
        cluster::SchemaRecordPtr & record,
        IngestMode ingest_mode,
        cluster::klog::AppendCallback callback,
        cluster::klog::CallbackData data,
        int64_t timeout_ms);

    int appendToNativeLog(
        cluster::SchemaRecordPtr & record,
        IngestMode /*ingest_mode*/,
        cluster::klog::AppendCallback callback,
        cluster::klog::CallbackData data,
        int64_t timeout_ms);


    void poll(Int32 timeout_ms);

    void cacheVirtualColumnNamesAndTypes();

    void updateLogStoreCodec(const String & settings_codec);

    ALWAYS_INLINE void checkReady() const;

    String getCurrentSNsSignature() const;

private:
    struct InitParams
    {
        bool attach;
        bool has_force_restore_data_flag;
        String date_column_name;
        MergingParams merging_params;
        StorageInMemoryMetadata metadata;
        ContextMutablePtr context;
    };
    std::unique_ptr<InitParams> init_params;

    /// is_local is a stream private to the local node
    bool is_local = false;

    UInt32 shards;
    bool rand_sharding_key = false;

    /// Direct vector of stream shards
    std::vector<StreamShardStorePtr> stream_shards;

    /// For sharding
    ExpressionActionsPtr sharding_key_expr;
    bool sharding_key_is_deterministic = false;
    String sharding_key_column_name;
    std::vector<UInt64> slot_to_shard;

    NamesAndTypesList virtual_column_names_and_types;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    mutable pcg64 rng;

    /// For ingest
    mutable std::atomic_uint_fast32_t next_shard = 0;

    /// Outstanding async ingest records
    UInt64 max_outstanding_blocks;
    std::atomic_uint_fast64_t outstanding_blocks = 0;

    std::atomic<bool> ready = false;

    UInt32 physical_shards = 0;

    CompressionMethodByte logstore_codec = CompressionMethodByte::NONE;

    std::atomic_flag inited;
    std::atomic_flag stopped;

    StreamIOStats metrics{MonotonicMilliseconds::now()};
};
}
