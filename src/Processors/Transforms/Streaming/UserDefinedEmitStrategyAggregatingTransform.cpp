#include <Processors/Transforms/Streaming/UserDefinedEmitStrategyAggregatingTransform.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>

#include <algorithm>

namespace DB
{
namespace Streaming
{

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : UserDefinedEmitStrategyAggregatingTransform(
          std::move(header), params_, std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id), 0, 1)
{
}

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant_, size_t max_threads_)
    : AggregatingTransform(
          std::move(header),
          std::move(params_),
          std::move(many_data_),
          current_variant_,
          max_threads_,
          "UserDefinedAggregatingTransform",
          ProcessorID::UserDefinedEmitStrategyAggregatingTransformID)
{
    chassert(params->params->group_by == IAggregatorParams::GroupBy::UserDefined);
}

void UserDefinedEmitStrategyAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    /// We don't need care other data variants, just finalize what we have
    auto & variants = *many_data->variants[current_variant];
    if (variants.empty())
        return;

    auto chunks = AggregatingHelper::convertToChunks(variants, *params);
    if (params->emit_version)
        emitVersion(chunks);

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);
}

String UserDefinedEmitStrategyAggregatingTransform::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "UserDefinedEmitStrategyAggregatingTransform";
        case AggregatorType::Hybrid:
            return "HybridUserDefinedEmitStrategyAggregatingTransform";
    }
}

}

}
