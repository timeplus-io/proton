#include "DistributedMergeTreeBlockOutputStream.h"

#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <Storages/StorageDistributedMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int OK;
}

DistributedMergeTreeBlockOutputStream::DistributedMergeTreeBlockOutputStream(
    StorageDistributedMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : storage(storage_), metadata_snapshot(metadata_snapshot_), query_context(query_context_)
{
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

    const auto & ingest_mode = query_context->getIngestMode();

    /// 2) Commit each sharded block to corresponding Kafka partition
    /// we failed the whole insert whenever single block failed
    for (auto & current_block : blocks)
    {
        IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(current_block.block)};
        record.partition_key = current_block.shard;
        if (!query_context->getIdempotentKey().empty())
        {
            record.setIdempotentKey(query_context->getIdempotentKey());
        }

        if (ingest_mode == "sync")
        {
            auto ret = storage.dwal->append(record, &DistributedMergeTreeBlockOutputStream::writeCallback, this, storage.dwal_append_ctx);
            if (ret != 0)
            {
                throw Exception("Failed to insert data", ret);
            }
            outstanding += 1;
        }
        else if (ingest_mode == "ordered")
        {
            auto ret = storage.dwal->append(record, storage.dwal_append_ctx);
            if (ret.err != ErrorCodes::OK)
            {
                throw Exception("Failed to insert data", ret.err);
            }
        }
        else
        {
            outstanding += 1;
            auto ret = storage.dwal->append(
                record,
                &StorageDistributedMergeTree::writeCallback,
                storage.writeCallbackData(query_context->getQueryStatusPollId(), outstanding),
                storage.dwal_append_ctx);
            if (ret != 0)
            {
                throw Exception("Failed to insert data", ret);
            }
        }
    }
}

void DistributedMergeTreeBlockOutputStream::writeCallback(const IDistributedWriteAheadLog::AppendResult & result)
{
    if (result.err != ErrorCodes::OK)
    {
        err = result.err;
    }
    else
    {
        ++committed;
    }
}

void DistributedMergeTreeBlockOutputStream::writeCallback(const IDistributedWriteAheadLog::AppendResult & result, void * data)
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

    /// 3) Inplace poll append result until either all of records have been committed or error out or timed out
    auto start = std::chrono::steady_clock::now();
    while (1)
    {
        if (committed == outstanding)
        {
            /// Successfully ingest all data
            return;
        }
        else if (err != ErrorCodes::OK)
        {
            throw Exception("Failed to insert data", err);
        }
        else
        {
            storage.dwal->poll(10, storage.dwal_append_ctx);
        }

        /// 30 seconds timeout
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() >= 30000)
        {
            throw Exception("Failed to insert data, timed out", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
}
}
