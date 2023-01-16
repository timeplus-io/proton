#include "UserDefinedEmitStrategyAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{
namespace Streaming
{

UserDefinedEmitStrategyAggregatingTransformWithSubstream::UserDefinedEmitStrategyAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "UserDefinedEmitStrategyAggregatingTransformWithSubstream",
        ProcessorID::UserDefinedEmitStrategyAggregatingTransformWithSubstreamID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED);
}

void UserDefinedEmitStrategyAggregatingTransformWithSubstream::finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    /// We don't need care other data variants, just finalize what we have
    assert(substream_ctx);

    auto & variants = substream_ctx->variants;
    if (variants.empty())
        return;

    Block block;
    if (params->final)
    {
        auto results = params->aggregator.convertToBlocksFinal(variants, ConvertAction::STREAMING_EMIT, 1);
        assert(results.size() == 1);
        block = std::move(results.back());

        if (params->emit_version)
            emitVersion(block, substream_ctx);
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
