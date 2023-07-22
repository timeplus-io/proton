#include <Processors/Transforms/Streaming/WindowAggregatingTransform.h>

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
WindowAggregatingTransform::WindowAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & log_name,
    ProcessorID pid_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        log_name,
        pid_)
{
    assert(params->params.window_params);
    assert(
        params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
        || params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END);
}

bool WindowAggregatingTransform::needFinalization(Int64 min_watermark) const
{
    if (min_watermark <= many_data->finalized_watermark.load(std::memory_order_relaxed))
        return false;

    auto local_windows_with_buckets = getLocalFinalizedWindowsWithBucketsImpl(min_watermark);

    /// In case when some lagged events arrived after timeout, we skip finalized windows and remove them later
    auto last_finalized_window_end = many_data->finalized_window_end.load(std::memory_order_relaxed);
    for (auto iter = local_windows_with_buckets.begin(); iter != local_windows_with_buckets.end();)
    {
        if (iter->window.end <= last_finalized_window_end)
            iter = local_windows_with_buckets.erase(iter);
        else
            break; /// No need check next elements, since windows is always sorted.
    }

    /// Has windows to finalize
    return !local_windows_with_buckets.empty();
}

bool WindowAggregatingTransform::prepareFinalization(Int64 min_watermark)
{
    if (min_watermark <= many_data->finalized_watermark.load(std::memory_order_relaxed))
        return false;

    /// After acquired finalizing lock
    prepared_windows_with_buckets.clear();
    for (auto * aggr_transform : many_data->aggregating_transforms)
    {
        auto * window_aggr_transform = reinterpret_cast<WindowAggregatingTransform *>(aggr_transform);
        auto windows_with_buckets = window_aggr_transform->getLocalFinalizedWindowsWithBucketsImpl(min_watermark);

        /// In case when some lagged events arrived after timeout, we skip finalized windows and remove them later
        auto last_finalized_window_end = many_data->finalized_window_end.load(std::memory_order_relaxed);
        for (auto iter = windows_with_buckets.begin(); iter != windows_with_buckets.end();)
        {
            if (iter->window.end <= last_finalized_window_end)
                iter = windows_with_buckets.erase(iter);
            else
                break; /// No need check next elements, since windows is always sorted.
        }

        for (auto & window_with_buckets : windows_with_buckets)
        {
            auto iter = prepared_windows_with_buckets.begin();
            for (; iter != prepared_windows_with_buckets.end(); ++iter)
            {
                if (iter->window.end == window_with_buckets.window.end)
                {
                    /// Unique merge buckets of same window (assume buckets always are sorted)
                    assert(iter->window.start == window_with_buckets.window.start);
                    std::vector<Int64> buckets;
                    buckets.reserve(std::max(iter->buckets.size(), window_with_buckets.buckets.size()));
                    std::ranges::set_union(iter->buckets, window_with_buckets.buckets, std::back_inserter(buckets));
                    iter->buckets = std::move(buckets);
                    break;
                }
                else if (iter->window.end > window_with_buckets.window.end)
                {
                    /// Merge a new window
                    iter = prepared_windows_with_buckets.emplace(iter, std::move(window_with_buckets));
                    break;
                }
            }

            /// Merge a new window into the back
            if (iter == prepared_windows_with_buckets.end())
                prepared_windows_with_buckets.push_back(std::move(window_with_buckets));
        }
    }

    /// Has windows to finalize
    return !prepared_windows_with_buckets.empty();
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void WindowAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    assert(chunk_ctx && chunk_ctx->hasWatermark());

    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(many_data->variants);
    if (prepared_data_ptr->empty())
        return;

    SCOPE_EXIT({ many_data->resetRowCounts(); });

    initialize(prepared_data_ptr);

    assert(prepared_data_ptr->at(0)->isTwoLevel());
    convertTwoLevel(prepared_data_ptr, chunk_ctx);
}

void WindowAggregatingTransform::initialize(ManyAggregatedDataVariantsPtr & data)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    assert(first->type != AggregatedDataVariants::Type::without_key && !params->params.overflow_row);

    /// At least we need one arena in first data item per thread
    Arenas & first_pool = first->aggregates_pools;
    for (size_t j = first_pool.size(); j < max_threads; j++)
        first_pool.emplace_back(std::make_shared<Arena>());
}

void WindowAggregatingTransform::convertTwoLevel(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;
    Block block;

    assert(!prepared_windows_with_buckets.empty());
    for (const auto & window_with_buckets : prepared_windows_with_buckets)
    {
        if (window_with_buckets.buckets.size() == 1)
        {
            block = params->aggregator.mergeAndConvertOneBucketToBlock(
                *data, first->aggregates_pool, params->final, ConvertAction::STREAMING_EMIT, window_with_buckets.buckets[0], &is_cancelled);
        }
        else
        {
            params->aggregator.mergeBuckets(
                *data, first->aggregates_pool, params->final, ConvertAction::INTERNAL_MERGE, window_with_buckets.buckets);
            block = params->aggregator.spliceAndConvertBucketsToBlock(
                *first, params->final, ConvertAction::INTERNAL_MERGE, window_with_buckets.buckets);
        }

        if (is_cancelled)
            return;

        if (needReassignWindow())
            reassignWindow(block, window_with_buckets.window);

        if (params->emit_version && params->final)
            emitVersion(block);

        if (merged_block)
        {
            assertBlocksHaveEqualStructure(merged_block, block, "merging buckets for streaming two level hashtable");
            for (size_t i = 0, size = merged_block.columns(); i < size; ++i)
            {
                const auto source_column = block.getByPosition(i).column;
                auto mutable_column = IColumn::mutate(std::move(merged_block.getByPosition(i).column));
                mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                merged_block.getByPosition(i).column = std::move(mutable_column);
            }
        }
        else
            merged_block = std::move(block);
    }

    many_data->finalized_window_end.store(prepared_windows_with_buckets.back().window.end, std::memory_order_relaxed);
    prepared_windows_with_buckets.clear();
    prepared_windows_with_buckets.shrink_to_fit();

    auto finalized_watermark = chunk_ctx->getWatermark();
    /// If is timeout, we set watermark after actual finalized last window end
    if (unlikely(finalized_watermark == TIMEOUT_WATERMARK))
    {
        finalized_watermark = many_data->finalized_window_end.load(std::memory_order_relaxed);
        chunk_ctx->setWatermark(finalized_watermark);
    }

    many_data->finalized_watermark.store(finalized_watermark, std::memory_order_relaxed);

    if (merged_block)
        setCurrentChunk(convertToChunk(merged_block), chunk_ctx);
}

void WindowAggregatingTransform::removeBuckets(Int64 finalized_watermark)
{
    /// Blocking finalization during remove buckets from current variant
    std::lock_guard lock(variants_mutex);
    removeBucketsImpl(finalized_watermark);
}

std::vector<Int64> WindowAggregatingTransform::getBucketsBefore(Int64 max_bucket) const
{
    /// Blocking finalization, it's a lightweight lock
    std::lock_guard lock(variants_mutex);
    return params->aggregator.bucketsBefore(variants, max_bucket);
}

}
}
