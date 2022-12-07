#include "TumbleHopAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
TumbleHopAggregatingTransformWithSubstream::TumbleHopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "TumbleHopAggregatingTransformWithSubstream",
        ProcessorID::TumbleHopAggregatingTransformWithSubstreamID)
{
    assert(
        (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        || (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END));
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void TumbleHopAggregatingTransformWithSubstream::finalize(SubstreamContext & ctx, ChunkContextPtr chunk_ctx)
{
    SCOPE_EXIT({ ctx.resetRowCounts(); });
    auto watermark = chunk_ctx->getWatermark();

    /// Finalize current watermark
    if (watermark.valid())
    {
        auto start = MonotonicMilliseconds::now();
        doFinalize(ctx, watermark, chunk_ctx);
        auto end = MonotonicMilliseconds::now();

        LOG_DEBUG(
            log,
            "Took {} milliseconds to finalize aggregation in substream id={}. watermark={}",
            end - start,
            watermark.id,
            watermark.watermark);

        /// Do memory arena recycling by last finalized watermark
        params->aggregator.removeBucketsBefore(ctx.variants, watermark);
    }
}

void TumbleHopAggregatingTransformWithSubstream::doFinalize(SubstreamContext & ctx, const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx)
{
    auto & data_variant = ctx.variants;

    if (data_variant.empty())
        return;

    assert(data_variant.isTwoLevel());

    Block merged_block;
    /// Figure out which buckets need get merged
    std::vector<size_t> buckets = params->aggregator.bucketsBefore(data_variant, watermark);
    for (auto bucket : buckets)
    {
        Block block;
        if (params->final)
        {
            block = params->aggregator.convertOneBucketToBlockFinal(data_variant, ConvertAction::STREAMING_EMIT, bucket);

            if (params->emit_version)
                emitVersion(ctx, block);
        }
        else
            block = params->aggregator.convertOneBucketToBlockIntermediate(data_variant, ConvertAction::STREAMING_EMIT, bucket);

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

    if (merged_block)
        setCurrentChunk(convertToChunk(merged_block), chunk_ctx);
}

}
}
