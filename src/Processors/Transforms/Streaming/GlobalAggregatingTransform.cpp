#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>

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
GlobalAggregatingTransform::GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : GlobalAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

GlobalAggregatingTransform::GlobalAggregatingTransform(
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
        "GlobalAggregatingTransform",
        ProcessorID::GlobalAggregatingTransformID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::OTHER);

    if (unlikely(params->params.overflow_row))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overflow row processing is not implemented in global aggregation");

    /// Need extra retracted data
    if (params->emit_changelog)
    {
        if (params->emit_version)
            throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");

        ManyRetractedDataVariants retracted_data(many_data->variants.size());
        for (auto & elem : retracted_data)
            elem = std::make_shared<AggregatedDataVariants>();

        many_data->setField(
            {std::move(retracted_data),
             /// Field serializer
             [this](const std::any & field, WriteBuffer & wb, VersionType) {
                 const auto & data = std::any_cast<const ManyRetractedDataVariants &>(field);
                 DB::writeIntBinary(data.size(), wb);
                 for (const auto & elem : data)
                     elem->serialize(wb, params->aggregator);
             },
             /// Field deserializer
             [this](std::any & field, ReadBuffer & rb, VersionType) {
                 auto & data = std::any_cast<ManyRetractedDataVariants &>(field);
                 size_t num;
                 DB::readIntBinary(num, rb);
                 data.resize(num);
                 for (auto & elem : data)
                 {
                     elem = std::make_shared<AggregatedDataVariants>();
                     elem->deserialize(rb, params->aggregator);
                 }
             }});
    }
}

bool GlobalAggregatingTransform::needFinalization(Int64 min_watermark) const
{
    if (min_watermark == INVALID_WATERMARK)
        return false;

    return true;
}

bool GlobalAggregatingTransform::prepareFinalization(Int64 min_watermark)
{
    if (min_watermark == INVALID_WATERMARK)
        return false;

    std::lock_guard lock(many_data->watermarks_mutex);
    if (std::ranges::all_of(many_data->watermarks, [](const auto & wm) { return wm != INVALID_WATERMARK; }))
    {
        /// Reset all watermarks to INVALID,
        /// Next finalization will just be triggered when all transform watermarks are updated
        std::ranges::for_each(many_data->watermarks, [](auto & wm) { wm = INVALID_WATERMARK; });
        return many_data->hasNewData(); /// If there is no new data, don't emit aggr result
    }
    return false;
}

std::pair<bool, bool> GlobalAggregatingTransform::executeOrMergeColumns(Chunk & chunk, size_t num_rows)
{
    if (params->emit_changelog)
    {
        assert(!params->only_merge && !no_more_keys);

        auto & retracted_variants = many_data->getField<ManyRetractedDataVariants>()[current_variant];
        auto & aggregated_variants = many_data->variants[current_variant];

        /// Blocking finalization during execution on current variant
        std::lock_guard lock(variants_mutex);
        return params->aggregator.executeAndRetractOnBlock(
            chunk.detachColumns(), 0, num_rows, *aggregated_variants, *retracted_variants, key_columns, aggregate_columns);
    }
    else
        return AggregatingTransform::executeOrMergeColumns(chunk, num_rows);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    SCOPE_EXIT({
        many_data->resetRowCounts();
        many_data->finalized_watermark.store(chunk_ctx->getWatermark(), std::memory_order_relaxed);
    });

    if (params->emit_changelog)
    {
        auto [retracted_chunk, chunk] = AggregatingHelper::mergeAndConvertToChangelogChunk(
            many_data->variants, many_data->getField<ManyRetractedDataVariants>(), *params);

        chunk.setChunkContext(chunk_ctx);
        setCurrentChunk(std::move(chunk), std::move(retracted_chunk));
    }
    else
    {
        auto chunk = AggregatingHelper::mergeAndConvertToChunk(many_data->variants, *params);
        if (params->emit_version && params->final)
            emitVersion(chunk);

        chunk.setChunkContext(chunk_ctx);
        setCurrentChunk(std::move(chunk));
    }
}

}
}
