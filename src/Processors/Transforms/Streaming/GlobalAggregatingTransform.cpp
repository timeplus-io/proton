#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
extern const int RECOVER_CHECKPOINT_FAILED;
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

    if (params->emit_changelog)
    {
        if (params->emit_version)
            throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");

        bool retract_enabled = false;
        many_data->setField(
            {retract_enabled,
             /// Field serializer
             [](const std::any & field, WriteBuffer & wb, [[maybe_unused]] VersionType version) {
                 assert(version >= V2);
                 DB::writeBinary(std::any_cast<bool>(field), wb);
             },
             /// Field deserializer
             [](std::any & field, ReadBuffer & rb, VersionType version) {
                 /// NOTE: Can not convert old impl to new impl V2
                 if (version < V2)
                     throw Exception(
                         ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                         "Failed to recover aggregation checkpoint with retract aggregated states from an incompatible version '{}'",
                         version);

                 DB::readBinary(std::any_cast<bool &>(field), rb);
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

        // Blocking finalization during execution on current variant
        std::lock_guard lock(variants_mutex);

        /// Enable retract after first finalization
        if (retractEnabled()) [[likely]]
            return params->aggregator.executeAndRetractOnBlock(
                chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns);
        else
            return params->aggregator.executeOnBlock(chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns);
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
        auto [retracted_chunk, chunk] = AggregatingHelper::mergeAndConvertToChangelogChunk(many_data->variants, *params);
        /// Enable retract after first finalization
        retractEnabled() |= chunk.rows();

        chunk.setChunkContext(chunk_ctx);
        setAggregatedResult(std::move(chunk), std::move(retracted_chunk));
    }
    else
    {
        ChunkList chunks;
        if (AggregatingHelper::onlyEmitUpdates(params->emit_mode))
            chunks = AggregatingHelper::mergeAndConvertUpdatesToChunks(many_data->variants, *params);
        else
            chunks = AggregatingHelper::mergeAndConvertToChunks(many_data->variants, *params);

        if (params->emit_version && params->final)
            emitVersion(chunks);

        if (chunks.empty()) [[unlikely]]
            chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

        chunks.back().setChunkContext(chunk_ctx); /// Set chunk context for the last chunk

        setAggregatedResult(std::move(chunks));
    }
}

bool & GlobalAggregatingTransform::retractEnabled() const noexcept
{
    return many_data->getField<bool &>();
}

}
}
