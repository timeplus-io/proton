#include <Interpreters/Streaming/RefCountDataBlockPage.h>
#include <Interpreters/Streaming/RefCountDataBlockPages.h>

namespace DB::Streaming
{
template <typename DataBlock>
void RefCountDataBlockPage<DataBlock>::pushBack(DataBlock && data_block)
{
    page.emplace_back(std::move(data_block));
    assert(page.size() <= pages->pageSize());
    ++active_data_blocks;
}

template <typename DataBlock>
void RefCountDataBlockPage<DataBlock>::deref(uint32_t page_offset) noexcept
{
    assert(page.size() > page_offset);
    page[page_offset].deref();

    if (page[page_offset].refCount() == 0)
    {
        pages->negateMetrics(page[page_offset].block);

        page[page_offset].clear();

        assert(active_data_blocks > 0);
        --active_data_blocks;
        if (active_data_blocks == 0)
            /// Try to gc this page
            pages->erasePage(this);
    }
}

}
