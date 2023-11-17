#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
}

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
    if (params->emit_changelog && params->emit_version)
        throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");
}

SubstreamContextPtr GlobalAggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    auto substream_ctx = AggregatingTransformWithSubstream::getOrCreateSubstreamContext(id);
    if (params->emit_changelog && !substream_ctx->hasField())
    {
        substream_ctx->setField(
            {std::make_shared<RetractedDataVariants>(),
            /// Field serializer
            [this](const std::any & field, WriteBuffer & wb) {
                const auto & data = std::any_cast<const RetractedDataVariantsPtr &>(field);
                params->aggregator.checkpoint(*data, wb);
            },
            /// Field deserializer
            [this](std::any & field, ReadBuffer & rb) {
                auto & data = std::any_cast<RetractedDataVariantsPtr &>(field);
                params->aggregator.recover(*data, rb);
            }});
    }
    return substream_ctx;
}

std::pair<bool, bool>
GlobalAggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    if (params->emit_changelog)
    {
        assert(!params->only_merge);

        auto num_rows = chunk.getNumRows();
        auto & retracted_variants = substream_ctx->getField<RetractedDataVariantsPtr>();
        auto & aggregated_variants = substream_ctx->variants;

        return params->aggregator.executeAndRetractOnBlock(
            chunk.detachColumns(), 0, num_rows, aggregated_variants, *retracted_variants, key_columns, aggregate_columns, no_more_keys);
    }
    else
        return AggregatingTransformWithSubstream::executeOrMergeColumns(chunk, substream_ctx);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransformWithSubstream::finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto finalized_watermark = chunk_ctx->getWatermark();
    SCOPE_EXIT({
        substream_ctx->resetRowCounts();
        substream_ctx->finalized_watermark = finalized_watermark;
    });

    /// If there is no new data, don't emit aggr result
    if (!substream_ctx->hasNewData())
        return;

    auto & variants = substream_ctx->variants;
    if (variants.empty())
        return;

    auto start = MonotonicMilliseconds::now();
    if (params->emit_changelog)
    {
        auto [retracted_chunk, chunk]
            = AggregatingHelper::convertToChangelogChunk(variants, *substream_ctx->getField<RetractedDataVariantsPtr>(), *params);
        setCurrentChunk(std::move(chunk), chunk_ctx, std::move(retracted_chunk));
    }
    else
    {
        auto chunk = AggregatingHelper::convertToChunk(variants, *params);
        if (params->final && params->emit_version)
            emitVersion(chunk, substream_ctx);

        setCurrentChunk(std::move(chunk), chunk_ctx);
    }
    auto end = MonotonicMilliseconds::now();

    LOG_INFO(log, "Took {} milliseconds to finalize aggregation", end - start);
}

}
}
