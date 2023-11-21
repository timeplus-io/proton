#pragma once

#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/RefCountDataBlockPage.h>

#include <deque>

namespace DB::Streaming
{

template <typename DataBlock>
struct RefCountDataBlockPages
{
    RefCountDataBlockPages(size_t page_size_, CachedBlockMetrics & metrics_) : page_size(page_size_), metrics(metrics_) { }

    ~RefCountDataBlockPages()
    {
        metrics.current_total_blocks -= block_pages.size();
        metrics.current_total_bytes -= total_bytes;
        metrics.total_blocks -= block_pages.size();
        metrics.total_bytes -= total_bytes;
        metrics.gced_blocks += block_pages.size();
    }

    void add(DataBlock && block)
    {
        updateMetrics(block);

        auto & current_page = block_pages.back();
        if (likely(current_page->size() < page_size))
        {
            current_page->page.emplace_back(std::move(block));
        }
        else
        {
            addPage();

            auto & page = block_pages.back();
            page->emplace_back(std::move(block));
        }
    }

    auto * lastPage() noexcept
    {
        assert(!block_pages.empty());
        return block_pages.back().get();
    }

    auto lastPageOffset() noexcept
    {
        assert(!block_pages.empty());
        assert(!block_pages.back()->empty());
        return block_pages.back()->size() - 1;
    }

    const DataBlock & lastDataBlock() const noexcept
    {
        assert(!block_pages.empty());
        return block_pages.back()->lastDataBlock();
    }

    int64_t minTimestamp() const noexcept { return min_ts; }
    int64_t maxTimestamp() const noexcept { return max_ts; }

    size_t pageSize() const noexcept { return page_size; }

    void addPage() { block_pages.emplace_back(std::make_unique<RefCountDataBlockPage<DataBlock>>(page_size)); }

    void erasePage(RefCountDataBlockPage<DataBlock> * page)
    {
        assert(page);
        if (unlikely(block_pages.size() == 1))
        {
            assert(page == block_pages.front().get());

            /// If this is the last page, keep it around
            page->clear();
            page->reserve(page_size);
            return;
        }

        auto iter
            = std::find_if(block_pages.begin(), block_pages.end(), [page](const auto & block_page) { return block_page.get() == page; });
        assert(iter != block_pages.end());
        block_pages.erase(iter);
    }

    void updateMetrics(const DataBlock & block) noexcept
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

    void negateMetrics(const DataBlock & block) noexcept
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

    CachedBlockMetrics & metrics;

private:
    int64_t min_ts = std::numeric_limits<int64_t>::max();
    int64_t max_ts = std::numeric_limits<int64_t>::min();
    size_t total_bytes = 0;
    size_t page_size;
    std::deque<RefCountDataBlockPagePtr<DataBlock>> block_pages;
};

}
