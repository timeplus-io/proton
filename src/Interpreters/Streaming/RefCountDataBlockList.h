#pragma once

#include <Core/LightChunk.h>
#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/RefCountDataBlock.h>
#include <Interpreters/Streaming/joinSerder_fwd.h>
#include <base/SerdeTag.h>
#include <base/defines.h>

#include <list>

namespace DB::Streaming
{

template <typename DataBlock>
struct RefCountDataBlockList
{
    /// \data_block_size_ : Number of rows per data block. If it is not zero, we may merge new data block to a existing one when pushing back new data block.
    ///    Merging into a bigger data block like (Chunk, LightChunk, Block) etc will have way better memory efficiency.
    RefCountDataBlockList(size_t data_block_size_, CachedBlockMetrics & metrics_) : data_block_size(data_block_size_), metrics(metrics_) { }

    explicit RefCountDataBlockList(CachedBlockMetrics & metrics_) : data_block_size(0), metrics(metrics_) { }

    ~RefCountDataBlockList()
    {
        metrics.current_total_blocks -= blocks.size();
        metrics.current_total_bytes -= total_bytes;
        metrics.total_blocks -= blocks.size();
        metrics.total_bytes -= total_bytes;
        metrics.gced_blocks += blocks.size();
    }

    void ALWAYS_INLINE updateMetrics(const DataBlock & block)
    {
        min_ts = std::min(block.minTimestamp(), min_ts);
        max_ts = std::max(block.maxTimestamp(), max_ts);

        /// Update metrics
        auto bytes = block.allocatedBytes();
        total_bytes += bytes;
        ++metrics.current_total_blocks;
        metrics.current_total_bytes += bytes;
        ++metrics.total_blocks;
        metrics.total_bytes += bytes;
    }

    void ALWAYS_INLINE negateMetrics(const DataBlock & block)
    {
        /// Update metrics
        auto bytes = block.allocatedBytes();
        total_bytes -= bytes;
        --metrics.current_total_blocks;
        metrics.current_total_bytes -= bytes;
        --metrics.total_blocks;
        metrics.total_bytes -= bytes;
        ++metrics.gced_blocks;
    }

    void erase(typename std::list<RefCountDataBlock<DataBlock>>::iterator iter)
    {
        assert(iter->refCount() == 0);
        negateMetrics(iter->block);
        blocks.erase(iter);
    }

    bool empty() const { return blocks.empty(); }

    auto lastDataBlockIter()
    {
        assert(!blocks.empty());
        /// return std::prev(blocks.end());
        return --blocks.end();
    }

    const DataBlock & lastDataBlock() const
    {
        assert(!blocks.empty());
        return blocks.back().block;
    }

    using iterator = typename std::list<RefCountDataBlock<DataBlock>>::iterator;
    using const_iterator = typename std::list<RefCountDataBlock<DataBlock>>::const_iterator;

    auto begin() { return blocks.begin(); }
    auto end() { return blocks.end(); }

    size_t size() const { return blocks.size(); }

    auto begin() const { return blocks.begin(); }
    auto end() const { return blocks.end(); }

    /// Push back the \data_block or merge \data_block to the current data block if concat is enabled.
    /// Return the starting row position for added \data_bock
    [[nodiscard]] size_t pushBackOrConcat(DataBlock && data_block)
    {
        if (data_block_size != 0)
        {
            if (!blocks.empty())
            {
                auto & last_data_block = blocks.back();
                if (last_data_block.rows() + data_block.rows() <= data_block_size)
                {
                    /// Merge to the last data block
                    negateMetrics(last_data_block.block);
                    auto starting_row = last_data_block.concat(std::move(data_block));
                    updateMetrics(last_data_block.block);
                    return starting_row;
                }
                else
                {
                    /// Eagerly reserve may have bad side effect that if the next data block will cause
                    /// total rows to exceed data_block_size, the current reservation will be wasted since
                    /// we will start a new data block.
                    if (data_block.rows() < data_block_size)
                        data_block.reserve(data_block_size);

                    /// Insert the current data block and reserve enough room for next merge
                    updateMetrics(data_block);
                    blocks.emplace_back(std::move(data_block));

                    return 0;
                }
            }
            else
            {
                if (data_block.rows() < data_block_size)
                    data_block.reserve(data_block_size);

                /// Insert the current data block
                updateMetrics(data_block);
                blocks.emplace_back(std::move(data_block));
                return 0;
            }
        }
        else
        {
            /// Merge feature is disabled
            updateMetrics(data_block);
            blocks.emplace_back(std::move(data_block));

            return 0;
        }
    }

    void pushBack(DataBlock && data_block)
    {
        assert(data_block_size == 0);
        updateMetrics(data_block);
        blocks.emplace_back(std::move(data_block));
    }

    Int64 minTimestamp() const noexcept { return min_ts; }
    Int64 maxTimestamp() const noexcept { return max_ts; }

    void serialize(const Block & header, WriteBuffer & wb, SerializedBlocksToIndices * serialized_blocks_to_indices = nullptr) const;
    void
    deserialize(const Block & header, ReadBuffer & rb, DeserializedIndicesToBlocks<DataBlock> * deserialized_indices_with_block = nullptr);

private:
    size_t data_block_size;
    SERDE Int64 min_ts = std::numeric_limits<Int64>::max();
    SERDE Int64 max_ts = std::numeric_limits<Int64>::min();
    SERDE size_t total_bytes = 0;

    SERDE std::list<RefCountDataBlock<DataBlock>> blocks;

    CachedBlockMetrics & metrics;
};

}
