#include "UserDefinedEmitStrategyAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
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

    auto chunks = AggregatingHelper::convertToChunks(variants, *params);

    if (params->emit_version && params->final)
        emitVersion(chunks, substream_ctx);

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);
}
}
}
