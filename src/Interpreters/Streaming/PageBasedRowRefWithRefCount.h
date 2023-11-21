#pragma once

#include <Interpreters/Streaming/RefCountDataBlockPages.h>

namespace DB::Streaming
{
/// Reference to the row in block with reference count
template <typename DataBlock>
struct PageBasedRowRefWithRefCount
{
    /// Which page the the current DataBlock resides in
    RefCountDataBlockPage<DataBlock> * page = nullptr;
    /// Offset inside the target page
    uint32_t page_offset = 0;
    /// Row number of the DataBlock
    uint32_t row_num = 0;

    /// We need this default ctor since hash table framework will call the default one initially
    PageBasedRowRefWithRefCount() = default;

    PageBasedRowRefWithRefCount(RefCountDataBlockPages<DataBlock> * block_pages, size_t row_num_)
        : page(block_pages->lastPage()), page_offset(block_pages->lastPageOffset()), row_num(static_cast<uint32_t>(row_num_))
    {
        assert(page);
    }

    PageBasedRowRefWithRefCount(const PageBasedRowRefWithRefCount & other)
        : page(other.page), page_offset(other.page_offset), row_num(other.row_num)
    {
        if (likely(page))
            page->ref(page_offset);
    }

    PageBasedRowRefWithRefCount & operator=(const PageBasedRowRefWithRefCount & other)
    {
        if (this == &other)
            return *this;

        /// First deref existing block count
        /// After deref, page may be GCed
        if (likely(page))
            page->deref(page_offset);

        page = other.page;
        page_offset = other.page_offset;
        row_num = other.row_num;

        if (likely(page))
            page->ref(page_offset);

        return *this;
    }

    const DataBlock & block() const
    {
        assert(page);
        return page->getDataBlock(page_offset);
    }

    ~PageBasedRowRefWithRefCount()
    {
        if (likely(page))
            page->deref(page_offset);
    }
};

}
