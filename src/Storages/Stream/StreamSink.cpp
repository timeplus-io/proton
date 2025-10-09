#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamSink.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/KafkaLog/KafkaWAL.h>
#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <base/ClockUtils.h>
#include <Common/ErrorCodes.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{
namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
extern const int OK;
}

StreamSink::StreamSink(std::shared_ptr<StorageStream> storage_, StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : SinkToStorage(
          query_context_->getSettingsRef().insert_allow_materialized_columns.value ? metadata_snapshot_->getSampleBlock()
                                                                                   : metadata_snapshot_->getSampleBlockNonMaterialized(),
          ProcessorID::StreamSinkID)
    , storage(std::move(storage_))
    , metadata_snapshot(metadata_snapshot_)
    , query_context(query_context_)
    , ingest_mode(getIngestMode())
    , versioned_column_positions(cluster::Nulls::NullVersion, {})
    , chunk_splitter(query_context->getSettingsRef().max_insert_block_size, getMaxInsertBlockBytes())
{
    ingest_state = std::make_shared<IngestState>();
}

const std::vector<UInt16> & StreamSink::getColumnPositions(UInt16 version_requested)
{
    auto & [version, column_positions] = versioned_column_positions;
    /// Reuse the precalculated columns positions if no metadata version was changed
    if (version_requested == version) [[likely]]
        return column_positions;

    /// Re-calculate the positions of columns to write in new version
    version = version_requested;

    /// Skipping calculate the positions of columns to write in new version if we only write partial columns,
    /// due to the old position will not be changed (only support add columns for now)
    if (!column_positions.empty())
        return column_positions;

    /// If old version is to write full columns
    /// we need check and update the positions of columns to write in new version
    assert(column_positions.empty());
    const auto & sink_block_header = getHeader();
    auto full_metadata_snapshot = storage->getInMemoryMetadataByVersion(version_requested);
    auto full_header = query_context->getSettingsRef().insert_allow_materialized_columns.value
        ? full_metadata_snapshot->getSampleBlock()
        : full_metadata_snapshot->getSampleBlockNonMaterialized();
    if (full_header.columns() != sink_block_header.columns())
    {
        /// light ingest
        assert(query_context->getSettingsRef().enable_light_ingest);

        column_positions.reserve(sink_block_header.columns());

        /// Figure out the column positions since we need encode them to file system
        for (const auto & col : sink_block_header)
            column_positions.push_back(full_header.getPositionByName(col.name));
    }

    return column_positions;
}

inline IngestMode StreamSink::getIngestMode() const
{
    if (auto current_ingest_mode = query_context->getIngestMode(); current_ingest_mode)
        return *current_ingest_mode;

    return storage->ingestMode();
}

UInt64 StreamSink::getMaxInsertBlockBytes() const
{
    const auto & settings = query_context->getSettingsRef();
    if (settings.max_insert_block_bytes.changed)
        return settings.max_insert_block_bytes.value;

    /// Use max_entry_size when max_insert_block_bytes is not specified
    return storage->maxEntrySize();
}

BlocksWithShard StreamSink::shardBlock(Block block) const
{
    if (storage->shards > 1 && storage->sharding_key_expr && !storage->rand_sharding_key)
    {
        auto selector = storage->createSelector(block);
        return chunk_splitter.splitBlock(std::move(block), storage->shards, selector);
    }

    /// Pick next shard to ingest this block
    auto shard = storage->getNextShardIndex();
    return chunk_splitter.splitBlockForSingleShard(std::move(block), shard);
}

namespace
{
/// Generate unique idempotent key for each chunk id and block id
inline std::string getBlockIdempotentKey(const std::string & idempotent_key, UInt32 chunk_id, size_t block_id)
{
    return fmt::format("{}-{}-{}", idempotent_key, chunk_id, block_id);
}
}

void StreamSink::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    auto current_chunk_id = chunk_id++;
    const auto & idem_key = query_context->getIdempotentKey();
    /// No exception for now
    // if (!idem_key.empty() && storage->shards > 1 && storage->rand_sharding_key) [[unlikely]]
    //     throw Exception(
    //         ErrorCodes::NOT_IMPLEMENTED,
    //         "Unexpected Operation: Using an idempotent key in a multi-shard stream with random sharding key can lead to data distribution "
    //         "inconsistencies. Please use a deterministic sharding key that matches your idempotent key requirements.");

    UInt16 schema_version = metadata_snapshot->getVersion();
    const auto & column_positions = getColumnPositions(schema_version);
    assert(column_positions.empty() || column_positions.size() == chunk.getNumColumns());

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    /// 1) Split block by sharding key and max block size/bytes.
    BlocksWithShard sharded_blocks{shardBlock(std::move(block))};

    /// Group splitted blocks by shard so same shard blocks are ingested in order.
    std::vector<std::vector<Block>> blocks_by_shard(storage->shards);
    size_t num_shards_with_blocks = 0;
    for (auto & bs : sharded_blocks)
    {
        if (blocks_by_shard[bs.shard].empty())
            ++num_shards_with_blocks;
        blocks_by_shard[bs.shard].push_back(std::move(bs.block));
    }

    static auto append_callback = [](const auto & result, const auto & data) {
        auto & state = *static_cast<IngestState *>(data.get());
        ++state.committed;
        if (result.err != ErrorCodes::OK)
            state.errcode = result.err;
    };

    /// 2) Commit each sharded block to corresponding streaming store partition
    size_t shard = 0;
    const auto & settings = query_context->getSettingsRef();
    const auto append_timeout_ms = settings.insert_timeout_ms.value;
    if (num_shards_with_blocks > 1 && settings.enable_concurrent_ingest.value)
    {
        /// Parallel ingest
        size_t threads = settings.max_insert_threads.value;
        if (threads == 0)
            threads = std::min<size_t>(num_shards_with_blocks - 1, 8 - 1); /// -1 for current ingest thread
        else
            threads = std::min(num_shards_with_blocks - 1, threads - 1);

        pool.emplace(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, threads, threads, /*max_queue_size_=*/num_shards_with_blocks);

        /// If shared blocks can be divided by threads completely, we like the current ingesting thread to
        /// take one more share since it needs wait for other threads to finish anyway.
        /// If it takes more shares, when it is done, probably the other worker threads are already done as well,
        /// this may result in lower latency.
        const size_t current_thread_share = (num_shards_with_blocks + threads) / (threads + 1);
        assert(current_thread_share < num_shards_with_blocks);
        for (size_t remaining_shards = num_shards_with_blocks; shard < blocks_by_shard.size() && remaining_shards > current_thread_share;
             ++shard)
        {
            if (!blocks_by_shard[shard].empty())
            {
                --remaining_shards;
                pool->scheduleOrThrow([this,
                                       shard,
                                       &column_positions,
                                       schema_version,
                                       &idem_key,
                                       current_chunk_id,
                                       append_timeout_ms,
                                       blocks = std::move(blocks_by_shard[shard]),
                                       thread_group = CurrentThread::getGroup()]() mutable {
                    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);

                    for (size_t block_id = 0; block_id < blocks.size(); ++block_id)
                    {
                        auto record = std::make_shared<cluster::SchemaRecord>(
                            cluster::protocol::OpCode::InsertData, std::move(blocks[block_id]), schema_version);

                        record->setColumnPositions(column_positions);
                        record->setShard(static_cast<uint32_t>(shard));

                        if (!idem_key.empty())
                            record->setIdempotentKey(getBlockIdempotentKey(idem_key, current_chunk_id, block_id));

                        ++outstanding;
                        storage->append(record, ingest_mode, append_callback, ingest_state, append_timeout_ms);
                    }
                });
            }
        }
        /// Fall through to sequence ingestion for the current thread
    }

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::InsertData, Block{}, schema_version);
    record->setColumnPositions(column_positions);

    for (const auto total_shards = blocks_by_shard.size(); shard < total_shards; ++shard)
    {
        auto & blocks = blocks_by_shard[shard];
        for (size_t block_id = 0, block_num = blocks.size(); block_id < block_num; ++block_id)
        {
            record->getBlock().swap(blocks[block_id]);
            record->setShard(static_cast<uint32_t>(shard));

            if (!idem_key.empty())
                record->setIdempotentKey(getBlockIdempotentKey(idem_key, current_chunk_id, block_id));

            ++outstanding;
            storage->append(record, ingest_mode, append_callback, ingest_state, append_timeout_ms);
        }
    }

    if (pool)
        pool->wait();

    /// we failed the whole insert whenever single block failed
    if (ingest_state->errcode != ErrorCodes::OK)
        throw DB::Exception(ingest_state->errcode, "Failed to ingest data, error='{}'", DB::ErrorCodes::getName(ingest_state->errcode));
}

void StreamSink::onFinish()
{
    return;
    /*
    /// We need wait for all outstanding ingesting block committed
    /// before dtor itself. Otherwise the if the registered callback is invoked
    /// after dtor, crash will happen
    if (!storage->kafka_log || ingest_mode != IngestMode::Sync)
        return;

    /// 3) Inplace poll append result until either all of records have been committed
    Stopwatch stopwatch;
    while (true)
    {
        if (ingest_state->committed == outstanding)
        {
            /// LOG_DEBUG(storage->log, "[sync] write a block done, written blocks={}, committed={}, error={}", outstanding, committed, errcode);
            if (ingest_state->errcode != ErrorCodes::OK)
                throw Exception(ingest_state->errcode, "Failed to insert data");

            return;
        }
        else
        {
            storage->poll(10);
        }

        if (stopwatch.elapsedSeconds() >= 2)
        {
            LOG_ERROR(
                storage->log, "Still Waiting for data to be committed. Appended data seems getting lost. There are probably having bugs.");
            stopwatch.restart();
        }
    }
        */
}

/// `checkpoint(...)` is a blocking operation, so the check interval cannot be too large
static constexpr auto CHECK_INTERVAL = std::chrono::milliseconds(10);
static constexpr int CHECKPOINT_TIMEOUT_SECONDS = 5;
void StreamSink::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    const auto checkpoint_timeout = MonotonicSeconds::now() + CHECKPOINT_TIMEOUT_SECONDS;

    std::unique_lock lock(mutex);
    while (true)
    {
        if (checkpoint_cv.wait_for(lock, CHECK_INTERVAL, [&] { return ingest_state->committed == outstanding; }))
        {
            if (ingest_state->errcode != ErrorCodes::OK)
                throw Exception(ingest_state->errcode, "Failed to checkpoint, appended data got error");

            ckpt_ctx->coordinator->checkpointed(getVersion(), getLogicID(), ckpt_ctx);

            /// Checkpointed, there is no new data coming in at this time, so we can reset outstanding/committed count
            outstanding = 0;
            ingest_state->committed.store(0);
            return;
        }
        else
        {
            storage->poll(10);
        }

        if (unlikely(MonotonicSeconds::now() > checkpoint_timeout))
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout for checkpoint, outstanding={}, committed={}, appended data seems getting lost.",
                outstanding,
                ingest_state->committed.load());
    }
}

}
