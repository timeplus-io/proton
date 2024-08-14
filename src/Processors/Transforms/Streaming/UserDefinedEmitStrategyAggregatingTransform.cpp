#include "UserDefinedEmitStrategyAggregatingTransform.h"

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{
namespace Streaming
{

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : UserDefinedEmitStrategyAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "UserDefinedAggregatingTransform",
        ProcessorID::UserDefinedEmitStrategyAggregatingTransformID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED);
}

void UserDefinedEmitStrategyAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    /// We don't need care other data variants, just finalize what we have
    auto & variants = *many_data->variants[current_variant];
    if (variants.empty())
        return;

    auto chunks = AggregatingHelper::convertToChunks(variants, *params);
    if (params->emit_version && params->final)
        emitVersion(chunks);

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);
}
}
}
