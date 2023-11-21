#pragma once

namespace DB::Streaming
{
template <typename DataBlock>
struct RefCountDataBlock
{
    /// init ref count to row count for RowRefsWithCount case
    /// When refcount drops to zero, which means nobody is referencing any row
    /// in the block so the block will be GCed
    explicit RefCountDataBlock(DataBlock && block_) : block(std::move(block_)), refcnt(static_cast<uint32_t>(block.rows())) { }
    explicit RefCountDataBlock(const DataBlock & block_) : block(block_), refcnt(static_cast<uint32_t>(block.rows())) { }

    RefCountDataBlock(RefCountDataBlock && other) noexcept : block(std::move(other.block)), refcnt(other.refcnt) { }

    RefCountDataBlock & operator=(RefCountDataBlock && other) noexcept
    {
        block = std::move(other.block);
        refcnt = other.refcnt;
        return *this;
    }

    void ref() noexcept
    {
        assert(block);
        ++refcnt;
    }

    void deref() noexcept
    {
        assert(block);
        assert(refcnt != 0);
        --refcnt;
    }

    uint32_t refCount() const noexcept { return refcnt; }

    void clear() noexcept
    {
        block.clear();
        refcnt = 0;
    }

    DataBlock block;
    uint32_t refcnt;
};

}
