#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
GlobalAggregatingTransform::GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : GlobalAggregatingTransform(std::move(header), params_, std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id), 0, 1)
{
}

GlobalAggregatingTransform::GlobalAggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant_, size_t max_threads_)
    : AggregatingTransform(
          std::move(header),
          std::move(params_),
          std::move(many_data_),
          current_variant_,
          max_threads_,
          "GlobalAggregatingTransform",
          ProcessorID::GlobalAggregatingTransformID)
{
    chassert(params->params->group_by == IAggregatorParams::GroupBy::Other);

    if (params->emit_changelog)
    {
        if (params->emit_version)
            throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");

        bool retract_enabled = false;
        if (!many_data->hasField())
        {
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
}

bool GlobalAggregatingTransform::needFinalization(Int64 min_watermark) const
{
    return min_watermark != INVALID_WATERMARK;
}

bool GlobalAggregatingTransform::prepareFinalization([[maybe_unused]] Int64 min_watermark)
{
    std::lock_guard lock(many_data->watermarks_mutex);

    if (params->emit_mode == EmitMode::OnUpdate)
    {
        /// If any updates of input are received, finalize the updates immediately
        bool has_updates = false;
        std::ranges::for_each(many_data->watermarks, [&](auto & wm) {
            has_updates |= (wm != INVALID_WATERMARK);
            wm = INVALID_WATERMARK;
        });

        return has_updates;
    }
    else
    {
        /// Received all periodic watermark of inputs
        /// If all periodic watermark of inputs are received, finalize if there is new data or repeat emit
        if (std::ranges::all_of(many_data->watermarks, [](const auto & wm) { return wm != INVALID_WATERMARK; }))
        {
            /// Reset all watermarks to INVALID,
            /// Next finalization will just be triggered when all transform watermarks are updated
            std::ranges::for_each(many_data->watermarks, [](auto & wm) { wm = INVALID_WATERMARK; });
            return params->repeatEmit() || many_data->hasNewData(); /// If there is new data or repeat emit, don't emit aggr result
        }
        return false;
    }
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    SCOPE_EXIT({
        many_data->resetRowCounts();
        many_data->finalized_watermark.store(chunk_ctx->getWatermark());
    });

    ChunkList chunks;
    if (params->emit_changelog)
    {
        chunks = AggregatingHelper::mergeAndConvertToChangelogChunks(
            many_data->variants, *params, std::move(many_data->processed_keys_columns));

        /// Enable retract after first finalization
        if (!chunks.empty())
            enableRetract();

        /// Enable keys cache when
        /// 1) Exists group by keys
        /// 2) And after first finalization, there are two reasons:
        ///   - If is first finalization after execution, all keys of current state are changed, so we don't need cache them.
        ///   - If is first finalization after recovery, the cached keys are not checkpointed, so we will lose the cached parts after recovery
        if (params->params->keys_size != 0 && params->aggregatorType() == AggregatorType::Memory)
            enableKeysCache();
    }
    else if (AggregatingHelper::onlyEmitUpdates(params->emit_mode))
    {
        chunks
            = AggregatingHelper::mergeAndConvertUpdatesToChunks(many_data->variants, *params, std::move(many_data->processed_keys_columns));
        if (params->emit_version)
            emitVersion(chunks);

        if (params->params->keys_size != 0 && params->aggregatorType() == AggregatorType::Memory)
            enableKeysCache();
    }
    else
    {
        chunks = AggregatingHelper::mergeAndConvertToChunks(many_data->variants, *params);
        if (params->emit_version)
            emitVersion(chunks);
    }

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);
}

bool GlobalAggregatingTransform::retractEnabled() const
{
    return many_data->hasField() && many_data->getField<bool>();
}

void GlobalAggregatingTransform::enableRetract()
{
    many_data->getField<bool &>() = true;
}


String GlobalAggregatingTransform::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
        {
            if (params->emit_changelog)
                return "ChangelogGlobalAggregatingTransform";
            else
                return "GlobalAggregatingTransform";
        }
        case AggregatorType::Hybrid:
        {
            if (params->emit_changelog)
                return "ChangelogHybridGlobalAggregatingTransform";
            else
                return "HybridGlobalAggregatingTransform";
        }
    }
}

}

}
