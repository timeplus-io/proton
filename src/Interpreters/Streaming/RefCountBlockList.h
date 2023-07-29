#pragma once

#include <Core/LightChunk.h>
#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/joinSerder_fwd.h>
#include <base/SerdeTag.h>
#include <base/defines.h>

#include <list>

namespace DB
{
namespace Streaming
{
template <typename DataBlock>
struct RefCountBlock
{
    /// init ref count to row count for RowRefsWithCount case
    /// When refcount drops to zero, which means nobody is referencing any row
    /// in the block so the block will be GCed
    explicit RefCountBlock(DataBlock && block_) : block(std::move(block_)), refcnt(static_cast<uint32_t>(block.rows())) { }
    explicit RefCountBlock(const DataBlock & block_) : block(block_), refcnt(static_cast<uint32_t>(block.rows())) { }

    RefCountBlock(RefCountBlock && other) noexcept : block(std::move(other.block)), refcnt(other.refcnt) { }

    RefCountBlock & operator=(RefCountBlock && other) noexcept
    {
        block = std::move(other.block);
        refcnt = other.refcnt;
        return *this;
    }

    void ref() { ++refcnt; }

    void deref()
    {
        assert(refcnt != 0);
        --refcnt;
    }

    UInt32 refCount() const { return refcnt; }

    DataBlock block;
    UInt32 refcnt;
};

template <typename DataBlock>
struct RefCountBlockList
{
    explicit RefCountBlockList(CachedBlockMetrics & metrics_) : metrics(metrics_) { }

    ~RefCountBlockList()
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

    void erase(typename std::list<RefCountBlock<DataBlock>>::iterator iter)
    {
        assert(iter->refCount() == 0);
        negateMetrics(iter->block);
        blocks.erase(iter);
    }

    bool empty() const { return blocks.empty(); }

    auto lastBlockIter()
    {
        assert(!blocks.empty());
        /// return std::prev(blocks.end());
        return --blocks.end();
    }

    const DataBlock & lastBlock() const
    {
        assert(!blocks.empty());
        return blocks.back().block;
    }

    using iterator = typename std::list<RefCountBlock<DataBlock>>::iterator;
    using const_iterator = typename std::list<RefCountBlock<DataBlock>>::const_iterator;

    auto begin() { return blocks.begin(); }
    auto end() { return blocks.end(); }

    size_t size() const { return blocks.size(); }

    auto begin() const { return blocks.begin(); }
    auto end() const { return blocks.end(); }

    void push_back(DataBlock block)
    {
        updateMetrics(block);
        blocks.emplace_back(std::move(block));
    }

    Int64 minTimestamp() const noexcept { return min_ts; }
    Int64 maxTimestamp() const noexcept { return max_ts; }

    void serialize(const Block & header, WriteBuffer & wb, SerializedBlocksToIndices * serialized_blocks_to_indices = nullptr) const;
    void
    deserialize(const Block & header, ReadBuffer & rb, DeserializedIndicesToBlocks<DataBlock> * deserialized_indices_with_block = nullptr);

private:
    SERDE Int64 min_ts = std::numeric_limits<Int64>::max();
    SERDE Int64 max_ts = std::numeric_limits<Int64>::min();
    SERDE size_t total_bytes = 0;

    SERDE std::list<RefCountBlock<DataBlock>> blocks;

    CachedBlockMetrics & metrics;
};

}
}
