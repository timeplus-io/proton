#include "StreamSink.h"
#include "StorageStream.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <KafkaLog/KafkaWAL.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int UNSUPPORTED_PARAMETER;
extern const int INTERNAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int OK;
}

StreamSink::StreamSink(StorageStream & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : SinkToStorage(
        query_context_->getSettingsRef().insert_allow_materialized_columns.value ? metadata_snapshot_->getSampleBlock()
                                                                                 : metadata_snapshot_->getSampleBlockNonMaterialized(),
        ProcessorID::StreamSinkID)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    /// , storage_snapshot(storage.getStorageSnapshot(metadata_snapshot))
    , query_context(query_context_)
{
    /// metadata_snapshot can contain only partial columns of the schema when light ingest feature is on
    /// Check this case here
    const auto & sink_block_header = getHeader();
    auto full_metadata_snapshot = storage_.getInMemoryMetadataPtr(metadata_snapshot->version);
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

    ingest_state = std::make_shared<IngestState>();
}

BlocksWithShard StreamSink::doShardBlock(Block block) const
{
    auto selector = storage.createSelector(block);

    Blocks sharded_blocks(storage.shards);

    for (Int32 shard_idx = 0; shard_idx < storage.shards; ++shard_idx)
        sharded_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns sharded_columns = block.getByPosition(col_idx_in_block).column->scatter(storage.shards, selector);
        for (Int32 shard_idx = 0; shard_idx < storage.shards; ++shard_idx)
            sharded_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(sharded_columns[shard_idx]);
    }

    BlocksWithShard blocks_with_shard;

    /// Filter out empty blocks
    for (size_t shard_idx = 0; shard_idx < sharded_blocks.size(); ++shard_idx)
    {
        if (sharded_blocks[shard_idx].rows())
            blocks_with_shard.emplace_back(std::move(sharded_blocks[shard_idx]), shard_idx);

        assert(sharded_blocks[shard_idx].rows() == 0);
    }

    return blocks_with_shard;
}

BlocksWithShard StreamSink::shardBlock(Block block) const
{
    Int32 shard = 0;
    if (storage.shards > 1)
    {
        if (storage.sharding_key_expr && !storage.rand_sharding_key)
            return doShardBlock(std::move(block));
        else
            /// Randomly pick one shard to ingest this block
            shard = storage.getNextShardIndex();
    }

    return {BlockWithShard{Block(std::move(block)), shard}};
}

inline IngestMode StreamSink::getIngestMode() const
{
    auto ingest_mode = query_context->getIngestMode();
    if (ingest_mode != IngestMode::None && ingest_mode != IngestMode::INVALID)
        return ingest_mode;

    auto storage_ingest_mode = storage.ingestMode();
    if (storage_ingest_mode != IngestMode::None && storage_ingest_mode != IngestMode::INVALID)
        return storage.ingestMode();

    return IngestMode::ASYNC;
}

void StreamSink::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    assert(column_positions.empty() || column_positions.size() == chunk.getNumColumns());

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    /// if (!storage_snapshot->object_columns.get()->empty())
    ///    convertDynamicColumnsToTuples(block, storage_snapshot);

    /// 1) Split block by sharding key.
    /// FIXME, when nativelog is distributed, we will need revisit the sharding logic
    BlocksWithShard blocks{shardBlock(std::move(block))};

    auto ingest_mode = getIngestMode();

    /// 2) Commit each sharded block to corresponding streaming store partition
    /// we failed the whole insert whenever single block failed
    /// proton: FIXME. Once we MVCC schema, each block will be bound to a schema version, for now, it is 0.
    UInt16 schema_version = 0;
    const auto & idem_key = query_context->getIdempotentKey();
    auto record = std::make_shared<nlog::Record>(nlog::OpCode::ADD_DATA_BLOCK, Block{}, schema_version);
    record->setColumnPositions(column_positions);

    static auto append_callback = [](const auto & result, const auto & data) {
        auto & state = *static_cast<IngestState *>(data.get());
        ++state.committed;
        if (result.err != ErrorCodes::OK)
            state.errcode = result.err;
    };

    for (auto & current_block : blocks)
    {
        record->getBlock().swap(current_block.block);
        record->setShard(static_cast<int32_t>(current_block.shard));

        if (!idem_key.empty())
            record->setIdempotentKey(idem_key);

        ++outstanding;
        storage.append(record, ingest_mode, append_callback, ingest_state, query_context->getBlockBaseId(), outstanding);
    }
}

void StreamSink::onFinish()
{
    /// We need wait for all outstanding ingesting block committed
    /// before dtor itself. Otherwise the if the registered callback is invoked
    /// after dtor, crash will happen
    if (!storage.kafka_log || getIngestMode() != IngestMode::SYNC)
        return;

    /// 3) Inplace poll append result until either all of records have been committed
    auto start = MonotonicSeconds::now();
    while (true)
    {
        if (ingest_state->committed == outstanding)
        {
            /// LOG_DEBUG(storage.log, "[sync] write a block done, written blocks={}, committed={}, error={}", outstanding, committed, errcode);
            if (ingest_state->errcode != ErrorCodes::OK)
                throw Exception("Failed to insert data", ingest_state->errcode);

            return;
        }
        else
        {
            storage.poll(10);
        }

        if (MonotonicSeconds::now() - start >= 2)
        {
            LOG_ERROR(
                storage.log, "Still Waiting for data to be committed. Appended data seems getting lost. There are probably having bugs.");
            start = MonotonicSeconds::now();
        }
    }
}

/// `checkpoint(...)` is a blocking operation, so the check interval cannot be too large
static constexpr auto CHECK_INTERVAL = std::chrono::milliseconds(10);
static constexpr int CHECKPOINT_TIMEOUT_SECONDS = 5;
void StreamSink::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    if (unlikely(getIngestMode() == IngestMode::FIRE_AND_FORGET))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for ingest mode 'FIRE_AND_FORGET'");

    const auto checkpoint_timeout = MonotonicSeconds::now() + CHECKPOINT_TIMEOUT_SECONDS;

    std::unique_lock lock(mutex);
    while (true)
    {
        if (checkpoint_cv.wait_for(lock, CHECK_INTERVAL, [&] { return ingest_state->committed == outstanding; }))
        {
            if (ingest_state->errcode != ErrorCodes::OK)
                throw Exception("Failed to checkpoint, appended data got error", ingest_state->errcode);

            ckpt_ctx->coordinator->checkpointed(getVersion(), getLogicID(), ckpt_ctx);

            /// Checkpointed, there is no new data coming in at this time, so we can reset outstanding/committed count
            outstanding = 0;
            ingest_state->committed.store(0);
            return;
        }
        else
        {
            storage.poll(10);
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
