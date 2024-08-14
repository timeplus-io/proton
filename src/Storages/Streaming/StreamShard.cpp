#include "StreamShard.h"
#include "StreamCallbackData.h"
#include "StreamingBlockReaderKafka.h"
#include "StreamingBlockReaderNativeLog.h"
#include "storageUtil.h"

#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALPool.h>
#include <NativeLog/Server/NativeLog.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <Common/ProtonCommon.h>
#include <Common/setThreadName.h>
#include <Common/timeScale.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INVALID_CONFIG_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
}

namespace
{

inline void assignSequenceID(nlog::RecordPtr & record)
{
    auto * col = record->getBlock().findByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
    if (!col)
        return;

    auto * seq_id_col = typeid_cast<ColumnInt64 *>(col->column->assumeMutable().get());
    if (seq_id_col)
    {
        auto & data = seq_id_col->getData();
        for (auto & item : data)
            item = record->getSN();
    }
}

inline void assignIndexTime(ColumnWithTypeAndName * index_time_col)
{
    if (!index_time_col)
        return;

    /// index_time_col->column
    ///    = index_time_col->type->createColumnConst(moved_block.rows(), nowSubsecond(3))->convertToFullColumnIfConst();

    const auto * type = typeid_cast<const DataTypeDateTime64 *>(index_time_col->type.get());
    if (type)
    {
        auto scale = type->getScale();
        auto * col = typeid_cast<ColumnDecimal<DateTime64> *>(index_time_col->column->assumeMutable().get());
        assert(col);

        auto now = nowSubsecond(scale).get<DateTime64>();
        auto & data = col->getData();
        for (auto & item : data)
            item = now;
    }
}
}

StreamShard::StreamShard(
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
    Poco::Logger * log_)
    : replication_factor(replication_factor_)
    , shards(shards_)
    , shard(shard_)
    , storage_stream(storage_stream_)
    , part_commit_pool(context_->getPartCommitPool())
    , log(log_)
{
    assert(storage_stream);

    if (!relative_data_path_.empty() && !isInmemory())
    {
        auto shard_path = fmt::format("{}{}/", relative_data_path_, shard);
        storage = StorageMergeTree::create(
            table_id_,
            shard_path,
            metadata_,
            attach_,
            context_,
            date_column_name_,
            merging_params_,
            std::move(settings_),
            has_force_restore_data_flag_,
            shard);

        buildIdempotentKeysIndex(storage->lastIdempotentKeys());
        auto sn = storage->committedSN();
        if (sn >= 0)
        {
            std::lock_guard lock(sns_mutex);
            last_sn = sn;
            local_sn = sn;
        }

        LOG_INFO(log, "Load committed sn={} for shard={} from path={}", sn, shard, shard_path);
    }
}

StreamShard::~StreamShard()
{
    shutdown();
}

void StreamShard::startup()
{
    source_multiplexers.reset(new StreamingStoreSourceMultiplexers(shared_from_this(), storage_stream->getContext(), log));

    initLog();

    /// for virtual tables or in-memory storage type, there is no storage object
    if (!storage)
        return;

    storage->startup();

    auto storage_settings = storage_stream->getSettings();
    if (storage_settings->storage_type.value == "streaming")
    {
        LOG_INFO(log, "Pure streaming storage mode, skip indexing data to historical store");
    }
    else
    {
        if (kafka)
        {
            if (storage_settings->logstore_subscription_mode.value == "shared")
            {
                /// Shared mode, register callback
                addSubscriptionKafka();
                LOG_INFO(log, "Tailing Kafka streaming store in shared subscription mode");
            }
            else
            {
                /// Dedicated mode has dedicated poll thread
                poller.emplace(1);
                poller->scheduleOrThrowOnError([this] { backgroundPollKafka(); });
                LOG_INFO(log, "Tailing Kafka streaming store in dedicated subscription mode");
            }
        }
        else
        {
            /// Dedicated mode has dedicated poll thread. nativelog only supports dedicate mode for now
            poller.emplace(1);
            poller->scheduleOrThrowOnError([this] { backgroundPollNativeLog(); });
            LOG_INFO(log, "Tailing NativeLog streaming store in dedicated subscription mode");
        }
    }
}

void StreamShard::shutdown()
{
    if (stopped.test_and_set())
        return;

    if (storage)
    {
        if (poller)
        {
            poller->wait();
            /// Force delete pool
            poller.reset();
        }
        else
        {
            removeSubscriptionKafka();
        }
        storage->shutdown();
    }

    if (kafka)
        kafka->shutdown();
}

void StreamShard::backgroundPollNativeLog()
{
    setThreadName("StreamShard");

    const auto & missing_sequence_ranges = storage->missingSequenceRanges();

    auto ssettings = storage_stream->getSettings();

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} flush_threshold_ms={} flush_threshold_count={} "
        "flush_threshold_bytes={} with missing_sequence_ranges={}",
        shard,
        snLoaded(),
        ssettings->flush_threshold_ms.value,
        ssettings->flush_threshold_count.value,
        ssettings->flush_threshold_bytes.value,
        sequenceRangesToString(missing_sequence_ranges));

    StreamCallbackData stream_commit{this, missing_sequence_ranges};

    StreamingBlockReaderNativeLog block_reader(
        shared_from_this(),
        snLoaded(),
        /*max_wait_ms*/ 1000,
        /*read_buf_size*/ 4 * 1024 * 1024,
        &stream_commit,
        nlog::ALL_SCHEMA,
        {},
        log);

    nlog::RecordPtrs batch;
    size_t batch_size = 100;
    batch.reserve(batch_size);

    size_t rows_threshold = ssettings->flush_threshold_count.value;
    size_t bytes_threshold = ssettings->flush_threshold_bytes.value;
    Int64 interval_threshold = ssettings->flush_threshold_ms.value;

    size_t current_bytes_in_batch = 0;
    size_t current_rows_in_batch = 0;
    auto last_batch_commit = MonotonicMilliseconds::now();

    while (!stopped.test())
    {
        try
        {
            /// Check if we have something to commit
            /// Every 10 seconds, flush the local file system checkpoint
            if (MonotonicSeconds::now() - last_commit_ts >= 10)
                periodicallyCommit();

            auto records{block_reader.read()};
            for (auto & record : records)
            {
                current_bytes_in_batch += record->totalSerializedBytes();
                current_rows_in_batch += record->getBlock().rows();
                batch.push_back(std::move(record));
            }

            if ((current_bytes_in_batch >= bytes_threshold) || (current_rows_in_batch >= rows_threshold)
                || (MonotonicMilliseconds::now() - last_batch_commit >= interval_threshold))
            {
                if (!batch.empty())
                {
                    stream_commit.commit(std::move(batch));

                    /// We like to re-init `batch` as after move, it's state is undefined
                    batch = nlog::RecordPtrs{};
                    batch.reserve(batch_size);

                    /// Reset
                    last_batch_commit = MonotonicMilliseconds::now();
                    current_bytes_in_batch = 0;
                    current_rows_in_batch = 0;
                }
            }
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to consume data for shard={}", shard));
        }
    }

    /// Final batch
    if (!batch.empty())
        stream_commit.commit(std::move(batch));

    stream_commit.wait();

    /// When tearing down, commit whatever it has
    finalCommit();
}

void StreamShard::backgroundPollKafka()
{
    /// Sleep a while to let librdkafka to populate topic / partition metadata
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    setThreadName("StreamShard");

    const auto & missing_sequence_ranges = storage->missingSequenceRanges();

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} flush_threshold_ms={} flush_threshold_count={} "
        "flush_threshold_bytes={} with missing_sequence_ranges={}",
        shard,
        kafka->consume_ctx.offset,
        kafka->consume_ctx.consume_callback_timeout_ms,
        kafka->consume_ctx.consume_callback_max_rows,
        kafka->consume_ctx.consume_callback_max_bytes,
        sequenceRangesToString(missing_sequence_ranges));

    callback_data = std::make_unique<StreamCallbackData>(this, missing_sequence_ranges);

    while (!stopped.test())
    {
        /// stop consume stream store records, if consume_blocker is set.
        if (consume_blocker.isCancelled())
            continue;

        try
        {
            auto err = kafka->log->consume(&StreamShard::consumeCallback, callback_data.get(), kafka->consume_ctx);
            if (err != ErrorCodes::OK)
            {
                LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, err);
                /// FIXME, more error code handling
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }

            /// Check if we have something to commit
            /// Every 10 seconds, flush the local file system checkpoint
            if (MonotonicSeconds::now() - last_commit_ts >= 10)
                periodicallyCommit();
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to consume data for shard={}", shard));
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    kafka->log->stopConsume(kafka->consume_ctx);

    callback_data->wait();

    /// When tearing down, commit whatever it has
    finalCommit();
}

void StreamShard::commitSNLocal(nlog::RecordSN commit_sn)
{
    try
    {
        storage->commitSN(commit_sn);
        last_commit_ts = MonotonicSeconds::now();

        LOG_INFO(log, "Committed sn={} for shard={} to local file system", commit_sn, shard);

        std::lock_guard lock(sns_mutex);
        local_sn = commit_sn;
    }
    catch (...)
    {
        /// It is ok as next commit will override this commit if it makes through
        LOG_ERROR(
            log,
            "Failed to commit sn={} for shard={} to local file system exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StreamShard::commitSNRemote(nlog::RecordSN commit_sn)
{
    if (!kafka)
        return;

    /// Commit sequence number to dwal
    try
    {
        Int32 err = 0;
        if (kafka->multiplexer)
        {
            klog::TopicPartitionOffset tpo{kafka->topic(), shard, commit_sn};
            err = kafka->multiplexer->commit(tpo);
        }
        else
        {
            err = kafka->log->commit(commit_sn, kafka->consume_ctx);
        }

        if (unlikely(err != 0))
            /// It is ok as next commit will override this commit if it makes through
            LOG_ERROR(log, "Failed to commit sequence={} for shard={} to dwal error={}", commit_sn, shard, err);
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit sequence={} for shard={} to dwal exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StreamShard::commitSN()
{
    size_t outstanding_sns_size = 0;
    size_t local_committed_sns_size = 0;

    nlog::RecordSN commit_sn = -1;
    Int64 outstanding_commits = 0;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != prev_sn)
        {
            outstanding_commits = last_sn - local_sn;
            commit_sn = last_sn;
            prev_sn = last_sn;
        }
        outstanding_sns_size = outstanding_sns.size();
        local_committed_sns_size = local_committed_sns.size();
    }

    LOG_DEBUG(
        log,
        "Sequence outstanding_sns_size={} local_committed_sns_size={} for shard={}",
        outstanding_sns_size,
        local_committed_sns_size,
        shard);

    if (commit_sn < 0)
        return;

    /// Commit sequence number to local file system every 100 records
    if (outstanding_commits >= 100)
        commitSNLocal(commit_sn);

    commitSNRemote(commit_sn);
}

inline void StreamShard::progressSequences(const SequencePair & seq)
{
    std::lock_guard lock(sns_mutex);
    progressSequencesWithoutLock(seq);
}

inline void StreamShard::progressSequencesWithoutLock(const SequencePair & seq)
{
    assert(!outstanding_sns.empty());

    if (seq != outstanding_sns.front())
    {
        /// Out of order committed sn
        local_committed_sns.insert(seq);
        return;
    }

    last_sn = seq.second;

    outstanding_sns.pop_front();

    /// Find out the max offset we can commit
    while (!local_committed_sns.empty())
    {
        auto & p = outstanding_sns.front();
        if (*local_committed_sns.begin() == p)
        {
            /// sn shall be consecutive
            assert(p.first == last_sn + 1);

            last_sn = p.second;

            local_committed_sns.erase(local_committed_sns.begin());
            outstanding_sns.pop_front();
        }
        else
        {
            break;
        }
    }

    assert(outstanding_sns.size() >= local_committed_sns.size());
    assert(last_sn >= prev_sn);

    storage->setInMemoryCommittedSN(last_sn);
}

void StreamShard::doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges)
{
    {
        std::lock_guard lock(sns_mutex);
        assert(seq_pair.first > last_sn);
        /// We are sequentially consuming records, so seq_pair is always increasing
        outstanding_sns.push_back(seq_pair);

        /// After deduplication, we may end up with empty block
        /// We still mark these deduped blocks committed and moving forward
        /// the offset checkpointing
        if (!block)
        {
            progressSequencesWithoutLock(seq_pair);
            return;
        }

        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// Commit blocks to file system async
    part_commit_pool.scheduleOrThrowOnError([&,
                                             moved_block = std::move(block),
                                             moved_seq = std::move(seq_pair),
                                             moved_keys = std::move(keys),
                                             moved_sequence_ranges = std::move(missing_sequence_ranges),
                                             this]() mutable {
        while (!stopped.test())
        {
            try
            {
                auto sink = storage->write(nullptr, storage->getInMemoryMetadataPtr(), storage_stream->getContext());

                /// Setup sequence numbers to persistent them to file system
                auto * merge_tree_sink = static_cast<MergeTreeSink *>(sink.get());
                merge_tree_sink->setSequenceInfo(std::make_shared<SequenceInfo>(moved_seq.first, moved_seq.second, moved_keys));
                merge_tree_sink->setMissingSequenceRanges(std::move(moved_sequence_ranges));

                merge_tree_sink->onStart();

                /// Reset index time here
                assignIndexTime(const_cast<ColumnWithTypeAndName *>(moved_block.findByName(ProtonConsts::RESERVED_INDEX_TIME)));

                merge_tree_sink->consume(Chunk(moved_block.getColumns(), moved_block.rows()));
                merge_tree_sink->onFinish();
                break;
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "Failed to commit rows={} for shard={} exception={} to file system",
                    moved_block.rows(),
                    shard,
                    getCurrentExceptionMessage(true, true));
                /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }

        progressSequences(moved_seq);
    });

    commitSN();
}

/// Add with lock held
inline void StreamShard::addIdempotentKey(const String & key)
{
    if (idempotent_keys.size() >= storage_stream->getContext()->getSettingsRef().max_idempotent_ids)
    {
        [[maybe_unused]] auto removed = idempotent_keys_index.erase(*idempotent_keys.front());
        assert(removed == 1);

        idempotent_keys.pop_front();
    }

    auto shared_key = std::make_shared<String>(key);
    idempotent_keys.push_back(shared_key);

    [[maybe_unused]] auto [_, inserted] = idempotent_keys_index.emplace(*shared_key);
    assert(inserted);

    assert(idempotent_keys.size() == idempotent_keys_index.size());
}

bool StreamShard::dedupBlock(const nlog::RecordPtr & record)
{
    if (!record->hasIdempotentKey())
        return false;

    auto & idem_key = record->idempotentKey();
    auto key_exists = false;
    {
        std::lock_guard lock{sns_mutex};
        key_exists = idempotent_keys_index.contains(idem_key);
        if (!key_exists)
            addIdempotentKey(idem_key);
    }

    if (key_exists)
    {
        LOG_INFO(log, "Skipping duplicate block, idempotent_key={} sn={}", idem_key, record->getSN());
        return true;
    }
    return false;
}


/// Merge `rhs` block to `lhs`
void StreamShard::mergeBlocks(Block & lhs, Block & rhs)
{
    /// FIXME, we are assuming schema is not changed
    assert(blocksHaveEqualStructure(lhs, rhs));

    /// auto lhs_rows = lhs.rows();

    for (size_t pos = 0; auto & rhs_col : rhs)
    {
        auto & lhs_col = lhs.getByPosition(pos);
        //        if (unlikely(lhs_col == nullptr))
        //        {
        //            /// lhs doesn't have this column
        //            ColumnWithTypeAndName new_col{rhs_col.cloneEmpty()};
        //
        //            /// what about column with default expression
        //            new_col.column->assumeMutable()->insertManyDefaults(lhs_rows);
        //            lhs.insert(std::move(new_col));
        //            lhs_col = lhs.findByName(rhs_col.name);
        //        }

        lhs_col.column->assumeMutable()->insertRangeFrom(*rhs_col.column.get(), 0, rhs_col.column->size());
        ++pos;
    }

    /// lhs.checkNumberOfRows();
}

void StreamShard::commit(nlog::RecordPtrs records, SequenceRanges missing_sequence_ranges)
{
    if (records.empty())
        return;

    Block block;
    auto keys = std::make_shared<IdempotentKeys>();

    Int64 start_sn = -1;

    for (auto & rec : records)
    {
        if (likely(rec->opcode() == nlog::OpCode::ADD_DATA_BLOCK))
        {
            if (dedupBlock(rec))
                continue;

            assignSequenceID(rec);

            if (likely(block))
            {
                /// Merge next block
                /// assign event sequence ID to block events
                mergeBlocks(block, rec->getBlock());
            }
            else
            {
                /// restart first block
                block.swap(rec->getBlock());
                start_sn = rec->getSN();
            }

            if (rec->hasIdempotentKey())
                keys->emplace_back(rec->getSN(), std::move(rec->idempotentKey()));

            /// If block contains JSON column, we will need commit to avoid block merge
            if (block.hasDynamicSubcolumns())
            {
                /// There are several cases we will need consider, [..] means commit as a group
                /// 1. all json blocks: [json_block], [json_block], [json_block], [json_block], [json_block]
                /// 2. all non-json blocks: [block], [block], [block], [block], [block]
                /// 3. interleaved-1: [json_block], [block, json_block], [block, json_block]
                /// 4. interleaved-2: [block, json_block], [block, json_block], [block]
                assert(start_sn >= 0 && rec->getSN() >= start_sn);

                LOG_DEBUG(
                    log, "Committing rows={} bytes={} for shard={} containing json column to file system", block.rows(), block.bytes(), shard);

                doCommit(
                    std::move(block),
                    std::make_pair(start_sn, rec->getSN()),
                    std::move(keys),
                    missing_sequence_ranges
                );

                /// Explicitly clear since std::move in theory can be implemented as no move
                block.clear();
                keys = std::make_shared<IdempotentKeys>();
            }
        }
        else if (rec->opcode() == nlog::OpCode::ALTER_DATA_BLOCK)
        {
            /// FIXME: execute the later before doing any ingestion
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Log opcode {} is not implemented", static_cast<uint8_t>(rec->opcode()));
        }
    }

    LOG_DEBUG(
        log, "Committing records={} rows={} bytes={} for shard={} to file system", records.size(), block.rows(), block.bytes(), shard);

    if (block)
    {
        assert(start_sn >= 0 && records.back()->getSN() >= start_sn);
        doCommit(
            std::move(block),
            std::make_pair(start_sn, records.back()->getSN()),
            std::move(keys),
            std::move(missing_sequence_ranges));
    }
}

inline void StreamShard::finalCommit()
{
    commitSN();

    nlog::RecordSN commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
            commit_sn = last_sn;
    }

    if (commit_sn >= 0)
        commitSNLocal(commit_sn);
}

inline void StreamShard::periodicallyCommit()
{
    nlog::RecordSN remote_commit_sn = -1;
    nlog::RecordSN commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
            commit_sn = last_sn;

        if (prev_sn != last_sn)
        {
            remote_commit_sn = last_sn;
            prev_sn = last_sn;
        }
    }

    if (commit_sn >= 0)
        commitSNLocal(commit_sn);

    if (remote_commit_sn >= 0)
        commitSNRemote(remote_commit_sn);

    last_commit_ts = MonotonicSeconds::now();
}

void StreamShard::consumeCallback(nlog::RecordPtrs records, klog::ConsumeCallbackData * data)
{
    auto * cdata = dynamic_cast<StreamCallbackData *>(data);

    if (records.empty())
    {
        cdata->streamShard()->periodicallyCommit();
        return;
    }

    cdata->commit(std::move(records));
}

void StreamShard::addSubscriptionKafka()
{
    const auto & missing_sequence_ranges = storage->missingSequenceRanges();
    callback_data = std::make_unique<StreamCallbackData>(this, missing_sequence_ranges);

    const auto & cluster_id = storage_stream->getSettings()->logstore_cluster_id.value;
    kafka->multiplexer = klog::KafkaWALPool::instance(storage_stream->getContext()).getOrCreateConsumerMultiplexer(cluster_id);

    klog::TopicPartitionOffset tpo{kafka->topic(), shard, snLoaded()};
    auto res = kafka->multiplexer->addSubscription(tpo, &StreamShard::consumeCallback, callback_data.get());
    if (res.err != ErrorCodes::OK)
        throw Exception("Failed to add subscription for shard=" + std::to_string(shard), res.err);

    kafka->shared_subscription_ctx = res.ctx;

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} in shared mode "
        "with missing_sequence_ranges={}",
        shard,
        tpo.offset,
        sequenceRangesToString(missing_sequence_ranges));
}

void StreamShard::removeSubscriptionKafka()
{
    if (!kafka || !kafka->multiplexer)
        /// It is possible that storage is called with `shutdown` without calling `startup`
        /// during system startup and partially deleted table gets cleaned up
        return;

    klog::TopicPartitionOffset tpo{kafka->topic(), shard, snLoaded()};
    auto res = kafka->multiplexer->removeSubscription(tpo);
    if (res != ErrorCodes::OK)
        throw Exception("Failed to remove subscription for shard=" + std::to_string(shard), res);

    callback_data->wait();

    while (!kafka->shared_subscription_ctx.expired())
    {
        LOG_INFO(log, "Waiting for subscription context to get away for shard={}", shard);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    LOG_INFO(log, "Removed subscription for shard={}", shard);

    finalCommit();
    kafka->multiplexer.reset();
}

void StreamShard::initLog()
{
    auto ssettings = storage_stream->getSettings();
    const auto & logstore = ssettings->logstore.value;

    auto context = storage_stream->getContext();
    if (logstore == ProtonConsts::LOGSTORE_NATIVE_LOG || nlog::NativeLog::instance(context).enabled() || isInmemory())
        initNativeLog();
    else if (logstore == ProtonConsts::LOGSTORE_KAFKA || klog::KafkaWALPool::instance(context).enabled())
        initKafkaLog();
    else
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Logstore type {} is not supported", logstore);
}

void StreamShard::initKafkaLog()
{
    auto context = storage_stream->getContext();

    if (!klog::KafkaWALPool::instance(context).enabled())
        return;

    LOG_INFO(log, "KafkaLog for shard={}", shard);

    auto ssettings = storage_stream->getSettings();

    const auto & storage_id = storage_stream->getStorageID();
    auto kafka_log = std::make_unique<KafkaLogContext>(
        toString(storage_id.uuid),
        shards,
        ssettings->logstore_replication_factor.value,
        context->getSettingsRef().async_ingest_block_timeout_ms,
        log);

    kafka.swap(kafka_log);

    const auto & offset_reset = ssettings->logstore_auto_offset_reset.value;
    if (offset_reset != "earliest" && offset_reset != "latest")
        throw Exception(
            "Invalid logstore_auto_offset_reset, only 'earliest' and 'latest' are supported", ErrorCodes::INVALID_CONFIG_PARAMETER);

    Int32 dwal_request_required_acks = 1;
    auto acks = static_cast<Int32>(ssettings->logstore_request_required_acks.value);
    if (acks >= -1 && acks <= ssettings->logstore_replication_factor.value)
        dwal_request_required_acks = acks;
    else
        throw Exception(
            "Invalid logstore_request_required_acks, shall be in [-1, " + std::to_string(ssettings->logstore_replication_factor.value)
                + "] range",
            ErrorCodes::INVALID_CONFIG_PARAMETER);

    Int32 dwal_request_timeout_ms = 30000;
    auto timeout = static_cast<Int32>(ssettings->logstore_request_timeout_ms.value);
    if (timeout > 0)
        dwal_request_timeout_ms = timeout;

    default_ingest_mode = toIngestMode(ssettings->distributed_ingest_mode.value);

    kafka->log = klog::KafkaWALPool::instance(context).get(ssettings->logstore_cluster_id.value);
    assert(kafka->log);

    if (storage)
    {
        /// Init consume context only when it has backing storage
        kafka->consume_ctx.partition = shard;
        kafka->consume_ctx.offset = snLoaded();
        /// To ensure the replica can consume records inserted after its down time.
        kafka->consume_ctx.enforce_offset = true;
        kafka->consume_ctx.auto_offset_reset = ssettings->logstore_auto_offset_reset.value;
        kafka->consume_ctx.consume_callback_timeout_ms = static_cast<int32_t>(ssettings->flush_threshold_ms.value);
        kafka->consume_ctx.consume_callback_max_rows = static_cast<int32_t>(ssettings->flush_threshold_count.value);
        kafka->consume_ctx.consume_callback_max_bytes = static_cast<int32_t>(ssettings->flush_threshold_bytes.value);
        kafka->log->initConsumerTopicHandle(kafka->consume_ctx);
    }

    /// Init produce context
    /// Cached ctx, reused by append. Multiple threads are accessing append context
    /// since librdkafka topic handle is thread safe, so we are good
    kafka->append_ctx.request_required_acks = dwal_request_required_acks;
    kafka->append_ctx.request_timeout_ms = dwal_request_timeout_ms;
    kafka->log->initProducerTopicHandle(kafka->append_ctx);
}

void StreamShard::initNativeLog()
{
    bool inmemory = isInmemory();

    auto & native_log = nlog::NativeLog::instance(storage_stream->getContext());
    if (!native_log.enabled())
    {
        if (inmemory)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER, "Pure in-memory storage type requires nativelog, but nativelog is disabled");

        return;
    }

    LOG_INFO(log, "NativeLog for shard={}", shard);

    auto ssettings = storage_stream->getSettings();
    const auto & storage_id = storage_stream->getStorageID();

    nlog::CreateStreamRequest request{storage_id.getTableName(), storage_id.uuid, shards, static_cast<UInt8>(replication_factor)};
    request.flush_messages = static_cast<int32_t>(ssettings->logstore_flush_messages.value);
    request.flush_ms = static_cast<int32_t>(ssettings->logstore_flush_ms.value);
    request.retention_bytes = ssettings->logstore_retention_bytes.value;
    request.retention_ms = ssettings->logstore_retention_ms.value;

    auto list_resp{native_log.listStreams(storage_id.getDatabaseName(), nlog::ListStreamsRequest(storage_id.getTableName()))};
    if (list_resp.hasError() || list_resp.streams.empty())
    {
        auto create_resp{native_log.createStream(storage_id.getDatabaseName(), request)};
        if (create_resp.hasError())
            throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create stream, error={}", create_resp.errString());
        else
            LOG_INFO(log, "Log was provisioned successfully");

        native_log.setInmemory(create_resp.ns, create_resp.stream, create_resp.id, create_resp.shards, inmemory);
    }
    else
    {
        /// Since we don't commit `inmemory` to metastore, we will need explicitly set this flag during bootstrap
        /// Whey we don't commit `inmemory` flag to metastore ? because this requires a metadata protocol upgrade
        /// Hack this for now
        for (const auto & stream_desc : list_resp.streams)
            native_log.setInmemory(stream_desc.ns, stream_desc.stream, stream_desc.id, stream_desc.shards, inmemory);
    }
}

void StreamShard::deinitNativeLog()
{
    auto & native_log = nlog::NativeLog::instance(storage_stream->getContext());
    if (!native_log.enabled())
        return;

    const auto & storage_id = storage_stream->getStorageID();
    nlog::DeleteStreamRequest request{storage_id.getTableName(), storage_id.uuid};
    for (Int32 i = 0; i < 3; ++i)
    {
        auto resp{native_log.deleteStream(storage_id.getDatabaseName(), request)};
        if (resp.hasError())
        {
            LOG_ERROR(log, "Failed to clean up log, error={}", resp.errString());
            if (resp.hasUnretryableError())
                break;

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        else
        {
            LOG_INFO(log, "Cleaned up log");
            break;
        }
    }
}

nlog::RecordSN StreamShard::snLoaded() const
{
    std::lock_guard lock(sns_mutex);
    if (local_sn >= 0)
        /// Sequence number committed on disk is sequence of a record
        /// `plus one` is the next sequence expecting
        return local_sn + 1;

    if (kafka)
        return -1000; /// STORED
    else
        return nlog::toSN(storage_stream->getSettings()->logstore_auto_offset_reset.value);
}

Int64 StreamShard::maxCommittedSN() const
{
    assert(storage);
    return storage->maxCommittedSN();
}

std::vector<Int64> StreamShard::getOffsets(const SeekToInfoPtr & seek_to_info) const
{
    assert(seek_to_info);
    seek_to_info->replicateForShards(shards);

    if (!seek_to_info->isTimeBased())
        return seek_to_info->getSeekPoints();

    if (kafka)
        return kafka->log->offsetsForTimestamps(kafka->topic(), seek_to_info->getSeekPoints());
    else
        return sequencesForTimestamps(seek_to_info->getSeekPoints());
}

std::vector<Int64> StreamShard::sequencesForTimestamps(std::vector<Int64> timestamps, bool append_time) const
{
    const auto & storage_id = storage_stream->getStorageID();

    std::vector<int32_t> shard_ids;
    shard_ids.reserve(shards);

    for (Int32 i = 0; i < shards; ++i)
        shard_ids.push_back(i);

    nlog::TranslateTimestampsRequest request{
        nlog::Stream{storage_id.getTableName(), storage_id.uuid}, std::move(shard_ids), std::move(timestamps), append_time};
    auto & native_log = nlog::NativeLog::instance(storage_stream->getContext());
    auto response{native_log.translateTimestamps(storage_id.getDatabaseName(), request)};
    if (response.hasError())
        throw Exception(response.error_code, "Failed to translate timestamps to record sequences");

    return response.sequences;
}

void StreamShard::buildIdempotentKeysIndex(const std::deque<std::shared_ptr<String>> & idempotent_keys_)
{
    idempotent_keys = idempotent_keys_;
    for (const auto & key : idempotent_keys)
        idempotent_keys_index.emplace(*key);
}

String StreamShard::logStoreClusterId() const
{
    return storage_stream->getSettings()->logstore_cluster_id.value;
}

nlog::RecordSN StreamShard::lastSN() const
{
    std::lock_guard lock(sns_mutex);
    return last_sn;
}

bool StreamShard::isMaintain() const
{
    return storage ? consume_blocker.isCancelled() && storage->isMaintain() : false;
}

void StreamShard::updateNativeLog()
{
    auto & native_log = nlog::NativeLog::instance(storage_stream->getContext());
    if (!native_log.enabled())
        return;

    const auto ssettings = storage->getSettings();
    const auto & storage_id = storage->getStorageID();

    std::map<std::string, std::int32_t> flush_settings
        = {{"flush_messages", ssettings->logstore_flush_messages.value}, {"flush_ms", ssettings->logstore_flush_ms.value}};

    std::map<std::string, std::int64_t> retention_settings
        = {{"retention_bytes", ssettings->logstore_retention_bytes.value}, {"retention_ms", ssettings->logstore_retention_ms.value}};

    nlog::UpdateStreamRequest request{storage_id.getTableName(), storage_id.uuid, std::move(flush_settings), std::move(retention_settings)};

    auto update_resp{native_log.updateStream(storage_id.getDatabaseName(), request)};
    if (update_resp.hasError())
        throw DB::Exception(update_resp.error_code, "Failed to update stream, error={}", update_resp.errString());
    else
        LOG_INFO(log, "NativeLog has been updated successfully");
}

bool StreamShard::isInmemory() const
{
    return storage_stream->isInmemory();
}
}
