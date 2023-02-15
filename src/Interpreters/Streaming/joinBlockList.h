#pragma once

#include <Interpreters/Streaming/joinMetrics.h>

#include <Core/Block.h>

namespace DB
{
namespace Streaming
{
struct RefCountBlock
{
    /// init ref count to row count for RowRefsWithCount case
    /// When refcount drops to zero, which means nobody is referencing any row
    /// in the block so the block will be GCed
    RefCountBlock(Block && block_) : block(std::move(block_)), refcnt(static_cast<uint32_t>(block.rows())) { }
    RefCountBlock(const Block & block_) : block(block_), refcnt(static_cast<uint32_t>(block.rows())) { }

    RefCountBlock(RefCountBlock && other) noexcept: block(std::move(other.block)), refcnt(other.refcnt) { }

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

    Block block;
    UInt32 refcnt;
};

struct JoinBlockList
{
    explicit JoinBlockList(JoinMetrics & metrics_) : metrics(metrics_) { }

    ~JoinBlockList()
    {
        metrics.current_total_blocks -= blocks.size();
        metrics.current_total_bytes -= total_bytes;
        metrics.total_blocks -= blocks.size();
        metrics.total_bytes -= total_bytes;
        metrics.gced_blocks += blocks.size();
    }

    void ALWAYS_INLINE updateMetrics(const Block & block)
    {
        min_ts = std::min(block.info.watermark_lower_bound, min_ts);
        max_ts = std::max(block.info.watermark, max_ts);

        /// Update metrics
        auto bytes = block.allocatedBytes();
        total_bytes += bytes;
        ++metrics.current_total_blocks;
        metrics.current_total_bytes += bytes;
        ++metrics.total_blocks;
        metrics.total_bytes += bytes;
    }

    void ALWAYS_INLINE negateMetrics(const Block & block)
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

    void erase(std::list<RefCountBlock>::iterator iter)
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

    const Block * lastBlock() const
    {
        assert(!blocks.empty());
        return &blocks.back().block;
    }

    using iterator = std::list<RefCountBlock>::iterator;
    using const_iterator = std::list<RefCountBlock>::const_iterator;

    iterator begin() { return blocks.begin(); }
    iterator end() { return blocks.end(); }

    size_t size() const { return blocks.size(); }

    const_iterator begin() const { return blocks.begin(); }
    const_iterator end() const { return blocks.end(); }

    void push_back(Block && block)
    {
        updateMetrics(block);
        blocks.push_back(std::move(block));
    }

    void push_back(const Block & block)
    {
        updateMetrics(block);
        blocks.push_back(block);
    }

    Int64 minTimestamp() const { return min_ts; }
    Int64 maxTimestamp() const { return max_ts; }

private:
    Int64 min_ts = std::numeric_limits<Int64>::max();
    Int64 max_ts = -1;
    size_t total_bytes = 0;

    std::list<RefCountBlock> blocks;

    JoinMetrics & metrics;
};

}
}
