#include <Processors/Streaming/SizedChunkSplitter.h>

#include <Common/PODArray.h>

#include <numeric>


namespace DB
{
namespace Streaming
{
BlocksWithShard SizedChunkSplitter::splitBlock(Block block, UInt32 num_shards, IColumn::Selector & selector) const
{
    /// `selector` is created by caller and initialized with the value same as shard id.
    /// Each shard is further split by max block size/bytes.
    /// `selector[i]` value is changed during the process to record the i-th row target block index.

    /// Each shard has a BlockMeter to record the current block's id and the its rows and bytes.
    struct BlockMeter
    {
        size_t rows = 0;
        size_t bytes = 0;
        Int64 id = -1;
    };
    std::vector<BlockMeter> shard_current_blocks(num_shards);

    /// Record the total number of split blocks and the block's shard
    size_t num_of_blocks = 0;
    std::vector<UInt32> blocks_shard;

    /// Iterate selector to get the shard id and check the shard's BlockMeter to see if the row is fit in current block or we need create a new block.
    for (size_t i = 0; i < selector.size(); ++i)
    {
        auto row_shard = selector[i];
        auto row_bytes = std::accumulate(block.cbegin(), block.cend(), static_cast<UInt64>(0), [i](UInt64 bytes, const auto & c) {
            return bytes + c.column->byteSizeAt(i);
        });

        if (shard_current_blocks[row_shard].id == -1 || shard_current_blocks[row_shard].rows >= max_rows
            || shard_current_blocks[row_shard].bytes + row_bytes > max_bytes)
        {
            /// Create a new block if row's shard has no block yet or its shard's current block reach the limit
            shard_current_blocks[row_shard] = {.rows = 0, .bytes = 0, .id = static_cast<Int64>(num_of_blocks++)};
            blocks_shard.push_back(static_cast<UInt32>(row_shard));
        }

        /// Update selector to the shard current block id and the block metrics
        selector[i] = shard_current_blocks[row_shard].id;
        shard_current_blocks[row_shard].rows++;
        shard_current_blocks[row_shard].bytes += row_bytes;
    }

    /// Create the split blocks by selector value
    Blocks sharded_blocks;
    sharded_blocks.reserve(num_of_blocks);
    for (size_t i = 0; i < num_of_blocks; ++i)
        sharded_blocks.push_back(block.cloneEmpty());

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns sharded_columns = block.getByPosition(col_idx_in_block).column->scatter(num_of_blocks, selector);
        for (size_t i = 0; i < num_of_blocks; ++i)
            sharded_blocks[i].getByPosition(col_idx_in_block).column = std::move(sharded_columns[i]);
    }

    BlocksWithShard blocks_with_shard;

    /// Filter out empty blocks
    for (size_t i = 0; i < num_of_blocks; ++i)
    {
        if (sharded_blocks[i].rows() > 0)
            blocks_with_shard.emplace_back(std::move(sharded_blocks[i]), blocks_shard[i]);
    }

    return blocks_with_shard;
}

BlocksWithShard SizedChunkSplitter::splitBlockForSingleShard(Block block, UInt32 shard) const
{
    /// Check if block max size/bytes requirement is met.
    auto block_rows = block.rows();
    if (block_rows == 1 || (block_rows <= max_rows && block.bytes() <= max_bytes))
        return {BlockWithShard{std::move(block), shard}};

    /// Split block to small blocks by max_insert_block_size and max_insert_block_bytes
    BlocksWithShard sharded_blocks;
    size_t start = 0;
    UInt64 current_bytes = 0;
    for (size_t row = 0; row < block_rows; ++row)
    {
        auto row_bytes = std::accumulate(block.cbegin(), block.cend(), static_cast<UInt64>(0), [row](UInt64 bytes, const auto & c) {
            return bytes + c.column->byteSizeAt(row);
        });

        if ((row - start >= max_rows || current_bytes + row_bytes > max_bytes) && row > start)
        {
            sharded_blocks.emplace_back(block.cloneWithCutColumns(start, row - start), shard);
            start = row;
            current_bytes = 0;
        }
        current_bytes += row_bytes;
    }
    sharded_blocks.emplace_back(block.cloneWithCutColumns(start, block.rows() - start), shard);
    return sharded_blocks;
}
}
}
