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

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void WindowAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    assert(chunk_ctx);
    watermark = chunk_ctx->getWatermark();
    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        if (isCancelled())
            return;

        /// The current transform is the last one in this round of
        /// finalization. Do watermark alignment for all of the variants
        /// pick the smallest watermark
        Int64 min_watermark{watermark};
        Int64 max_watermark{watermark};

        std::vector<Int64 *> timeout_watermarks;
        for (auto & wm : many_data->watermarks)
        {
            if (unlikely(wm == TIMEOUT_WATERMARK))
            {
                timeout_watermarks.emplace_back(&wm);
                continue;
            }

            if (wm < min_watermark)
                min_watermark = wm;

            if (wm > max_watermark)
                max_watermark = wm;
        }

        if (min_watermark != max_watermark)
            LOG_INFO(log, "Found watermark skew. min_watermark={}, max_watermark={}", min_watermark, max_watermark);

        auto start = MonotonicMilliseconds::now();
        doFinalize(min_watermark, chunk_ctx);
        auto end = MonotonicMilliseconds::now();

        LOG_INFO(log, "Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        /// Aligning watermark
        auto finalized_watermark = many_data->finalized_watermark;
        for (auto * timeout_watermark : timeout_watermarks)
            *timeout_watermark = finalized_watermark;

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();

        /// We first notify all other variants that the aggregation is done for this round
        /// and then remove the project window buckets and their memory arena for the current variant.
        /// This save a bit time and a bit more efficiency because all variants can do memory arena
        /// recycling in parallel.
        removeBucketsImpl(finalized_watermark);
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        if (!isCancelled())
            many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "StreamingAggregated. Took {} milliseconds to wait for finalizing {} shard aggregation",
            end - start,
            many_data->variants.size());

        removeBucketsImpl(many_data->finalized_watermark);
    }
}

void WindowAggregatingTransform::doFinalize(Int64 watermark, const ChunkContextPtr & chunk_ctx)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(many_data->variants);
    if (prepared_data_ptr->empty())
        return;

    SCOPE_EXIT({ many_data->resetRowCounts(); });

    initialize(prepared_data_ptr);

    assert(prepared_data_ptr->at(0)->isTwoLevel());
    convertTwoLevel(prepared_data_ptr, watermark, chunk_ctx);
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

void WindowAggregatingTransform::convertTwoLevel(ManyAggregatedDataVariantsPtr & data, Int64 watermark, const ChunkContextPtr & chunk_ctx)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;
    Block block;

    const auto & last_finalized_windows_with_buckets = getFinalizedWindowsWithBuckets(many_data->finalized_watermark);
    const auto & windows_with_buckets = getFinalizedWindowsWithBuckets(watermark);
    for (const auto & window_with_buckets : windows_with_buckets)
    {
        /// In case when some lagged events arrived after timeout, we skip finalized windows
        if (!last_finalized_windows_with_buckets.empty()
            && window_with_buckets.window.end <= last_finalized_windows_with_buckets.back().window.end)
            continue;

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

    if (watermark != TIMEOUT_WATERMARK)
        many_data->finalized_watermark = watermark;
    /// If is timeout, we set watermark after actual finalized last window
    else if (!windows_with_buckets.empty())
        many_data->finalized_watermark = windows_with_buckets.back().window.end;

    if (merged_block)
    {
        chunk_ctx->setWatermark(many_data->finalized_watermark);
        setCurrentChunk(convertToChunk(merged_block), chunk_ctx);
    }
}

}
}
