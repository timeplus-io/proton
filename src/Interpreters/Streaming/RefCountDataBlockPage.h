#pragma once

#include <Interpreters/Streaming/RefCountDataBlock.h>

namespace DB::Streaming
{

template <typename DataBlock>
struct RefCountDataBlockPages;

template <typename DataBlock>
struct RefCountDataBlockPage
{
    explicit RefCountDataBlockPage(RefCountDataBlockPages<DataBlock> *);

    void ref(uint32_t page_offset) noexcept
    {
        assert(page.size() > page_offset);
        page[page_offset].ref();
    }

    void deref(uint32_t page_offset) noexcept;

    const DataBlock & lastDataBlock() const noexcept
    {
        assert(!page.empty());
        return page.back().block;
    }

    const DataBlock & getDataBlock(uint32_t page_offset) noexcept
    {
        assert(page.size() > page_offset);
        return page[page_offset].block;
    }

    void pushBack(DataBlock && data_block);

    void clear() { page.clear(); }

    void reserve(size_t page_size) { page.reserve(page_size); }

    auto size() const noexcept { return page.size(); }
    auto empty() const noexcept { return page.empty(); }
    auto activeDataBlocks() const noexcept { return active_data_blocks; }

private:
    RefCountDataBlockPages<DataBlock> * pages;
    std::vector<RefCountDataBlock<DataBlock>> page;
    size_t active_data_blocks = 0;
};

template <typename RefCountDataBlock>
using RefCountDataBlockPagePtr = std::unique_ptr<RefCountDataBlockPage<RefCountDataBlock>>;
}
