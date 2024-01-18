#pragma once

#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/RefCountDataBlockPage.h>

#include <base/defines.h>

#include <deque>

namespace DB::Streaming
{

template <typename DataBlock>
struct RefCountDataBlockPages
{
    RefCountDataBlockPages(size_t page_size_, CachedBlockMetrics & metrics_) : metrics(metrics_), page_size(page_size_) { addPage(); }

    ~RefCountDataBlockPages()
    {
        for (const auto & page : block_pages)
        {
            metrics.total_blocks -= page->activeDataBlocks();
            metrics.gced_blocks += page->activeDataBlocks();
        }

        metrics.total_rows -= total_rows;
        metrics.total_metadata_bytes -= total_metadata_bytes;
        metrics.total_data_bytes -= total_data_bytes;
    }

    void pushBack(DataBlock && block)
    {
        assert(current_page);

        updateMetrics(block);

        if (likely(current_page->size() < page_size))
        {
            current_page->pushBack(std::move(block));
        }
        else
        {
            addPage();
            current_page->pushBack(std::move(block));
        }
    }

    auto * lastPage() noexcept
    {
        assert(current_page);
        return current_page;
    }

    auto lastPageOffset() noexcept
    {
        assert(!block_pages.empty());
        assert(!block_pages.back()->empty());
        return current_page->size() - 1;
    }

    const DataBlock & lastDataBlock() const noexcept
    {
        assert(current_page);
        return current_page->lastDataBlock();
    }

    int64_t minTimestamp() const noexcept { return min_ts; }
    int64_t maxTimestamp() const noexcept { return max_ts; }

    size_t pageSize() const noexcept { return page_size; }
    size_t size() const noexcept { return block_pages.size(); }

    ALWAYS_INLINE void addPage()
    {
        block_pages.emplace_back(std::make_unique<RefCountDataBlockPage<DataBlock>>(this));
        current_page = block_pages.back().get();
    }

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
        auto rows = block.rows();
        auto allocated_metadata_bytes = block.allocatedMetadataBytes();
        auto allocated_data_bytes = block.allocatedDataBytes();
        total_rows += rows;
        total_metadata_bytes += allocated_metadata_bytes;
        total_data_bytes += allocated_data_bytes;

        ++metrics.total_blocks;
        metrics.total_rows += rows;
        metrics.total_metadata_bytes += allocated_metadata_bytes;
        metrics.total_data_bytes += allocated_data_bytes;
    }

    void negateMetrics(const DataBlock & block) noexcept
    {
        /// Update metrics
        auto rows = block.rows();
        auto allocated_metadata_bytes = block.allocatedMetadataBytes();
        auto allocated_data_bytes = block.allocatedDataBytes();
        total_rows -= rows;
        total_metadata_bytes -= allocated_metadata_bytes;
        total_data_bytes -= allocated_data_bytes;

        --metrics.total_blocks;
        metrics.total_rows -= rows;
        metrics.total_metadata_bytes -= allocated_metadata_bytes;
        metrics.total_data_bytes -= allocated_data_bytes;
        ++metrics.gced_blocks;
    }

    CachedBlockMetrics & metrics;

private:
    int64_t min_ts = std::numeric_limits<int64_t>::max();
    int64_t max_ts = std::numeric_limits<int64_t>::min();
    size_t total_rows = 0;
    size_t total_metadata_bytes = 0;
    size_t total_data_bytes = 0;
    size_t page_size;
    std::deque<RefCountDataBlockPagePtr<DataBlock>> block_pages;
    RefCountDataBlockPage<DataBlock> * current_page = nullptr;
};

}
