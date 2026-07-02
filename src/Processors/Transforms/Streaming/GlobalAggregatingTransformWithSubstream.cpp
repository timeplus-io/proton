#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>

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
GlobalAggregatingTransformWithSubstream::GlobalAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, size_t id)
    : AggregatingTransformWithSubstream(
          std::move(header),
          std::move(params_),
          id,
          "GlobalAggregatingTransformWithSubstream",
          ProcessorID::GlobalAggregatingTransformWithSubstreamID)
{
    chassert(params->params->group_by == IAggregatorParams::GroupBy::Other);

    if (params->emit_changelog && params->emit_version)
        throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");
}

void GlobalAggregatingTransformWithSubstream::initSubstreamContext(const SubstreamAggregatedDataPtr & substream_ctx)
{
    if (params->emit_changelog && !substream_ctx->hasField())
    {
        bool retract_enabled = false;
        substream_ctx->setField(
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

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransformWithSubstream::finalize(const SubstreamAggregatedDataPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto finalized_watermark = chunk_ctx->getWatermark();
    SCOPE_EXIT({
        substream_ctx->resetRowCounts();
        substream_ctx->finalized_watermark = finalized_watermark;
    });

    /// If there is no new data, don't emit aggr result
    if (!(params->repeatEmit() || substream_ctx->hasNewData()))
        return;

    auto & variants = substream_ctx->variants;
    if (variants->empty())
        return;

    Stopwatch stopwatch;

    ChunkList chunks;
    if (params->emit_changelog)
    {
        chunks = AggregatingHelper::convertToChangelogChunks(*variants, *params, std::move(substream_ctx->processed_keys_columns));
        /// Enable retract after first finalization
        if (!chunks.empty())
            enableRetract(substream_ctx);

        /// Enable keys cache when
        /// 1) Exists group by keys
        /// 2) And after first finalization, there are two reasons:
        ///   - If is first finalization after execution, all keys of current state are changed, so we don't need cache them.
        ///   - If is first finalization after recovery, the cached keys are not checkpointed, so we will lose the cached parts after recovery
        if (params->params->keys_size != 0 && params->aggregatorType() == AggregatorType::Memory)
            enableKeysCache(substream_ctx);
    }
    else if (AggregatingHelper::onlyEmitUpdates(params->emit_mode))
    {
        chunks = AggregatingHelper::convertUpdatesToChunks(*variants, *params, std::move(substream_ctx->processed_keys_columns));
        if (params->emit_version)
            emitVersion(chunks, substream_ctx);

        if (params->params->keys_size != 0 && params->aggregatorType() == AggregatorType::Memory)
            enableKeysCache(substream_ctx);
    }
    else
    {
        chunks = AggregatingHelper::convertToChunks(*variants, *params);
        if (params->emit_version)
            emitVersion(chunks, substream_ctx);
    }

    if (chunks.empty()) [[unlikely]]
        chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    /// Set chunk context for the last chunk
    chunks.back().setChunkContext(chunk_ctx);
    setAggregatedResult(chunks);

    if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms >= 100)
    {
        auto [aggr_chunks, aggr_rows] = chunksAndRowsOfAggregateResults();
        LOG_INFO(
            logger,
            "Took {} milliseconds to finalize aggregation, aggregated_chunks={} aggregated_rows={}",
            elapsed_ms,
            aggr_chunks,
            aggr_rows);
    }
}

bool GlobalAggregatingTransformWithSubstream::retractEnabled(const SubstreamAggregatedDataPtr & substream_ctx) const
{
    return substream_ctx->hasField() && substream_ctx->getField<bool>();
}

void GlobalAggregatingTransformWithSubstream::enableRetract(const SubstreamAggregatedDataPtr & substream_ctx)
{
    substream_ctx->getField<bool &>() = true;
}

String GlobalAggregatingTransformWithSubstream::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "GlobalAggregatingTransformWithSubstream";
        case AggregatorType::Hybrid:
            return "HybridGlobalAggregatingTransformWithSubstream";
    }
}

}

}
