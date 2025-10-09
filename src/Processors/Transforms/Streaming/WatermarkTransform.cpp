#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_EMIT_MODE;
}

namespace Streaming
{
WatermarkTransform::WatermarkTransform(const Block & header, EmitParamsPtr params_, bool skip_stamping_for_backfill_data_)
    : ISimpleTransform(header, header, false, ProcessorID::WatermarkTransformID)
    , params(std::move(params_))
    , skip_stamping_for_backfill_data(skip_stamping_for_backfill_data_)
    , logger(getLogger("WatermarkTransform"))
{
    chassert(params->mode != EmitMode::None);
    watermark = std::make_unique<WatermarkStamper>(*params, logger);
    assert(watermark);
    watermark->preProcess(header);
}

void WatermarkTransform::transform(Chunk & chunk)
{
    chunk.clearWatermark();

    if (chunk.isHistoricalDataStart() && skip_stamping_for_backfill_data) [[unlikely]]
    {
        mute_watermark = true;
        return;
    }

    if (chunk.isHistoricalDataEnd() && skip_stamping_for_backfill_data) [[unlikely]]
    {
        mute_watermark = false;
        watermark->processAfterUnmuted(chunk);
        return;
    }

    if (chunk.avoidWatermark())
        return;

    if (mute_watermark)
        watermark->processWithMutedWatermark(chunk);
    else
        watermark->process(chunk);
}

void WatermarkTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(hasState());
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) { watermark->serialize(wb); });
}

void WatermarkTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType, ReadBuffer & rb) { watermark->deserialize(rb); });
}
}
}
