#include <Processors/Transforms/Streaming/UserDefinedEmitStrategyAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/AggregatingHelper.h>

namespace DB
{
namespace Streaming
{

UserDefinedEmitStrategyAggregatingTransformWithSubstream::UserDefinedEmitStrategyAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, size_t id)
    : AggregatingTransformWithSubstream(
          std::move(header),
          std::move(params_),
          id,
          "UserDefinedEmitStrategyAggregatingTransformWithSubstream",
          ProcessorID::UserDefinedEmitStrategyAggregatingTransformWithSubstreamID)
{
    assert(params->params->group_by == IAggregatorParams::GroupBy::UserDefined);
}

void UserDefinedEmitStrategyAggregatingTransformWithSubstream::finalize(
    const SubstreamAggregatedDataPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    /// We don't need care other data variants, just finalize what we have
    assert(substream_ctx);

    if (substream_ctx->variants->empty())
        return;

    auto chunks = AggregatingHelper::convertToChunks(*substream_ctx->variants, *params);

    if (params->emit_version)
        emitVersion(chunks, substream_ctx);

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);
}

String UserDefinedEmitStrategyAggregatingTransformWithSubstream::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "UserDefinedEmitStrategyAggregatingTransformWithSubstream";
        case AggregatorType::Hybrid:
            return "HybridUserDefinedEmitStrategyAggregatingTransformWithSubstream";
    }
}

}

}
