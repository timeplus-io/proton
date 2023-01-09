#pragma once

#include "IngestingBlocks.h"
#include "StreamCallbackData.h"
#include "StreamingStoreSourceMultiplexer.h"

#include <base/ClockUtils.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <KafkaLog/KafkaWAL.h>
#include <KafkaLog/KafkaWALConsumerMultiplexer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Common/ThreadPool.h>

#include <pcg_random.hpp>

namespace DB
{
class StorageMergeTree;
struct KafkaLogContext;

/// StreamShard contains 2 parts
/// 1. one shard for streaming log store
/// 2. one shard for historical store (optional).
/// A background process consumes data from log store shard and then commits the data
/// to the shard of historical store. Please note that the historical store shard is optional.
class StreamShard final : public std::enable_shared_from_this<StreamShard>
{
public:
    StreamShard(
        Int32 replication_factor_,
        Int32 shards_,
        Int32 shard_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeData::MergingParams & merging_params_,
        std::unique_ptr<StreamSettings> settings_,
        bool has_force_restore_data_flag_,
        MergeTreeData * storage_stream_,
        Poco::Logger * log_);

    ~StreamShard();

    void startup();
    void shutdown();

    nlog::RecordSN lastSN() const;

    String logStoreClusterId() const;

    const IStorage * storageStream() const { return storage_stream; }

    const StorageMergeTree * getStorage() const { return storage ? storage.get() : nullptr; }

    IngestMode ingestMode() const { return default_ingest_mode; }

    bool isMaintain() const;

    bool isLogStoreKafka() const { return kafka != nullptr; }

    void updateNativeLog();

    bool isInmemory() const;

    Int32 getShard() const { return shard; }

    std::pair<String, Int32> getStreamShard() const { return {toString(storage_stream->getStorageID().uuid), shard}; }

private:
    void initLog();
    void initKafkaLog();
    void initNativeLog();
    void deinitNativeLog();

    nlog::RecordSN snLoaded() const;

    void backgroundPollKafka();
    void backgroundPollNativeLog();

    void addSubscriptionKafka();
    void removeSubscriptionKafka();

    static void mergeBlocks(Block & lhs, Block & rhs);

    bool dedupBlock(const nlog::RecordPtr & record);
    void addIdempotentKey(const String & key);
    void buildIdempotentKeysIndex(const std::deque<std::shared_ptr<String>> & idempotent_keys_);

    using SequencePair = std::pair<nlog::RecordSN, nlog::RecordSN>;

    void commit(nlog::RecordPtrs records, SequenceRanges missing_sequence_ranges);
    void doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges);
    void commitSN();
    void commitSNLocal(nlog::RecordSN commit_sn);
    void commitSNRemote(nlog::RecordSN commit_sn);

    void finalCommit();
    void periodicallyCommit();

    void progressSequences(const SequencePair & seq);
    void progressSequencesWithoutLock(const SequencePair & seq);
    Int64 maxCommittedSN() const;

    std::vector<Int64> sequencesForTimestamps(std::vector<Int64> timestamps, bool append_time = false) const;

    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info) const;

    static void consumeCallback(nlog::RecordPtrs records, klog::ConsumeCallbackData * data);

    friend struct StreamCallbackData;
    friend class StorageStream;

private:
    Int32 replication_factor;
    Int32 shards;
    Int32 shard;

    MergeTreeData * storage_stream;

    IngestMode default_ingest_mode{};

    /// Cached ctx for reuse
    std::unique_ptr<KafkaLogContext> kafka;

    /// Local checkpoint threshold timer
    Int64 last_commit_ts = MonotonicSeconds::now();

    /// Forwarding storage if it is not virtual
    std::shared_ptr<StorageMergeTree> storage;
    std::optional<ThreadPool> poller;

    ThreadPool & part_commit_pool;

    mutable std::mutex sns_mutex;
    nlog::RecordSN last_sn = -1; /// To be committed to logstore
    nlog::RecordSN prev_sn = -1; /// Committed to logstore
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

    Poco::Logger * log;

    std::atomic_flag stopped;

    /// ActionBlocker for consuming stream store records
    ActionBlocker consume_blocker;
};

struct KafkaLogContext
{
    KafkaLogContext(const String & topic_, Int32 shards_, Int32 replication_factor_, Int64 timeout_ms_, Poco::Logger * log_)
        : append_ctx(topic_, shards_, replication_factor_)
        , consume_ctx(topic_, shards_, replication_factor_)
        , ingesting_blocks(timeout_ms_, log_)
    {
    }

    ~KafkaLogContext() { shutdown(); }

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

}
