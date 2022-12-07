#include "GlobalAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
GlobalAggregatingTransformWithSubstream::GlobalAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "GlobalAggregatingTransformWithSubstream",
        ProcessorID::GlobalAggregatingTransformWithSubstreamID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::OTHER);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransformWithSubstream::finalize(SubstreamContext & ctx, ChunkContextPtr chunk_ctx)
{
    /// If there is no new data, don't emit aggr result
    if (!ctx.hasNewData())
        return;

    auto start = MonotonicMilliseconds::now();
    doFinalize(ctx, chunk_ctx);
    auto end = MonotonicMilliseconds::now();

    LOG_INFO(log, "Took {} milliseconds to finalize aggregation", end - start);
}

void GlobalAggregatingTransformWithSubstream::doFinalize(SubstreamContext & ctx, ChunkContextPtr & chunk_ctx)
{
    SCOPE_EXIT({ ctx.resetRowCounts(); });

    auto & variants = ctx.variants;
    if (variants.empty())
        return;

    Block block;
    if (params->final)
    {
        auto results = params->aggregator.convertToBlocksFinal(variants, ConvertAction::STREAMING_EMIT, 1);
        assert(results.size() == 1);
        block = std::move(results.back());

        if (params->emit_version)
            emitVersion(ctx, block);
    }
    else
    {
        auto results = params->aggregator.convertToBlocksIntermediate(variants, ConvertAction::STREAMING_EMIT, 1);
        assert(results.size() == 1);
        block = std::move(results.back());
    }

    if (block)
        setCurrentChunk(convertToChunk(block), chunk_ctx);
}

}
}
