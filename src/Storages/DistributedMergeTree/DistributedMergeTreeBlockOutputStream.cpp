#include "DistributedMergeTreeBlockOutputStream.h"
#include "StorageDistributedMergeTree.h"

#include <DistributedWriteAheadLog/KafkaWAL.h>
#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <common/ClockUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int OK;
    extern const int UNSUPPORTED_PARAMETER;
}

DistributedMergeTreeBlockOutputStream::DistributedMergeTreeBlockOutputStream(
    StorageDistributedMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : storage(storage_), metadata_snapshot(metadata_snapshot_), query_context(query_context_)
{
}

DistributedMergeTreeBlockOutputStream::~DistributedMergeTreeBlockOutputStream()
{
    /// We need wait for all outstanding ingesting block committed
    /// before dtor itself. Otherwise the if the registered callback is invoked
    /// after dtor, crash will happen
    flush();
}

Block DistributedMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

BlocksWithShard DistributedMergeTreeBlockOutputStream::doShardBlock(const Block & block) const
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
        {
            /// FIXME, further split sharded blocks by size to avoid big block
            blocks_with_shard.emplace_back(std::move(sharded_blocks[shard_idx]), shard_idx);
        }
    }

    return blocks_with_shard;
}

BlocksWithShard DistributedMergeTreeBlockOutputStream::shardBlock(const Block & block) const
{
    size_t shard = 0;
    if (storage.shards > 1)
    {
        if (storage.sharding_key_expr)
        {
            /// FIXME, if sharding key is `rand`, then we don't need shard block
            /// since we can randomly pick one shard to ingest this block. This is
            /// especially true when there are N shards and the block has only M rows
            /// where N > M in which case each shard will be assigned at most 1 row
            /// which is not good.
            return doShardBlock(block);
        }
        else
        {
            /// Randomly pick one shard to ingest this block
            shard = storage.getRandomShardIndex();
        }
    }

    return {BlockWithShard{Block(block), shard}};
}

inline String DistributedMergeTreeBlockOutputStream::getIngestMode() const
{
    auto ingest_mode = query_context->getIngestMode();

    if (!ingest_mode.empty())
        return ingest_mode;

    if (!storage.default_ingest_mode.empty())
        return storage.default_ingest_mode;

    return "async";
}

void DistributedMergeTreeBlockOutputStream::write(const Block & block)
{
    if (block.rows() == 0)
    {
        return;
    }

    /// 1) Split block by sharding key
    BlocksWithShard blocks{shardBlock(block)};

    /// FIXME, if one block is too large in size (bigger than max size of a Kafka record can have),
    /// further split the bock

    auto ingest_mode = getIngestMode();

    /// 2) Commit each sharded block to corresponding Kafka partition
    /// we failed the whole insert whenever single block failed
    for (auto & current_block : blocks)
    {
        DWAL::Record record{DWAL::OpCode::ADD_DATA_BLOCK, std::move(current_block.block)};
        record.partition_key = current_block.shard;
        if (!query_context->getIdempotentKey().empty())
        {
            record.setIdempotentKey(query_context->getIdempotentKey());
        }

        if (ingest_mode == "async")
        {
            LOG_TRACE(storage.log,
                    "[async] write a block={} rows={} shard={} query_status_poll_id={} ...",
                    outstanding, record.block.rows(), current_block.shard, query_context->getQueryStatusPollId());
            auto callback_data = storage.writeCallbackData(query_context->getQueryStatusPollId(), outstanding);
            auto ret = storage.dwal->append(
                record,
                &StorageDistributedMergeTree::writeCallback,
                callback_data.get(),
                storage.dwal_append_ctx);
            if (ret == ErrorCodes::OK)
            {
                /// The writeCallback takes over the ownership of callback data
                callback_data.release();
            }
            else
            {
                throw Exception("Failed to insert data async", ret);
            }
        }
        else if (ingest_mode == "sync")
        {
            LOG_TRACE(storage.log,
                    "[sync] write a block={} rows={} shard={} committed={} ...",
                    outstanding, record.block.rows(), current_block.shard, committed);
            auto ret = storage.dwal->append(record, &DistributedMergeTreeBlockOutputStream::writeCallback, this, storage.dwal_append_ctx);
            if (ret != 0)
            {
                throw Exception("Failed to insert data sync", ret);
            }
        }
        else if (ingest_mode == "fire_and_forget")
        {
            LOG_TRACE(storage.log,
                    "[fire_and_forget] write a block={} rows={} shard={} ...",
                    outstanding, record.block.rows(), current_block.shard);
            auto ret = storage.dwal->append(record, nullptr, nullptr, storage.dwal_append_ctx);
            if (ret != 0)
            {
                throw Exception("Failed to insert data fire_and_forget", ret);
            }
        }
        else if (ingest_mode == "ordered")
        {
            /// ordered
            LOG_TRACE(storage.log,
                    "[ordered] write a block={} rows={} shard={} ...",
                    outstanding, record.block.rows(), current_block.shard);
            auto ret = storage.dwal->append(record, storage.dwal_append_ctx);
            if (ret.err != ErrorCodes::OK)
            {
                throw Exception("Failed to insert data ordered", ret.err);
            }
            LOG_TRACE(storage.log,
                    "[ordered] write a block={} rows={} shard={} done",
                    outstanding, record.block.rows(), current_block.shard);
        }
        else
        {
            throw Exception("Failed to insert data, non-support ingest mode: " + ingest_mode, ErrorCodes::UNSUPPORTED_PARAMETER);
        }
        outstanding += 1;
    }
}

void DistributedMergeTreeBlockOutputStream::writeCallback(const DWAL::AppendResult & result)
{
    ++committed;
    if (result.err != ErrorCodes::OK)
    {
        errcode = result.err;
    }
    LOG_TRACE(storage.log, "[sync] write a block done, and current committed={}, error={}", committed, errcode);
}

void DistributedMergeTreeBlockOutputStream::writeCallback(const DWAL::AppendResult & result, void * data)
{
    auto stream = static_cast<DistributedMergeTreeBlockOutputStream *>(data);
    stream->writeCallback(result);
}

void DistributedMergeTreeBlockOutputStream::flush()
{
    if (query_context->getIngestMode() != "sync")
    {
        return;
    }

    /// 3) Inplace poll append result until either all of records have been committed
    auto start = MonotonicSeconds::now();
    while (1)
    {
        if (committed == outstanding)
        {
            LOG_DEBUG(storage.log, "[sync] write a block done, writed blocks={}, committed={}, error={}", outstanding, committed, errcode);
            if (errcode != ErrorCodes::OK)
            {
                throw Exception("Failed to insert data", errcode);
            }
            return;
        }
        else
        {
            storage.dwal->poll(10, storage.dwal_append_ctx);
        }

        if (MonotonicSeconds::now() - start >= 2)
        {
            LOG_ERROR(
                storage.log, "Still Waiting for data to be committed. Appended data seems getting lost. There are probably having bugs.");
            start = MonotonicSeconds::now();
        }
    }
}
}
