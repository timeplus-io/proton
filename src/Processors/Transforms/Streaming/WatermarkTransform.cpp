#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Processors/Transforms/Streaming/HopWatermarkStamper.h>
#include <Processors/Transforms/Streaming/SessionWatermarkStamper.h>
#include <Processors/Transforms/Streaming/TumbleWatermarkStamper.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_EMIT_MODE;
}

namespace Streaming
{
namespace
{
WatermarkStamperPtr initWatermark(const WatermarkStamperParams & params, Poco::Logger * logger)
{
    assert(params.mode != EmitMode::None);
    if (params.window_params)
    {
        switch (params.window_params->type)
        {
            case WindowType::TUMBLE:
                return std::make_unique<TumbleWatermarkStamper>(params, logger);
            case WindowType::HOP:
                return std::make_unique<HopWatermarkStamper>(params, logger);
            case WindowType::SESSION:
                return std::make_unique<SessionWatermarkStamper>(params, logger);
            default:
                break;
        }
    }
    return std::make_unique<WatermarkStamper>(params, logger);
}
}

WatermarkTransform::WatermarkTransform(
    const Block & header, WatermarkStamperParamsPtr params_, bool skip_stamping_for_backfill_data_, Poco::Logger * logger)
    : ISimpleTransform(header, header, false, ProcessorID::WatermarkTransformID)
    , params(std::move(params_))
    , skip_stamping_for_backfill_data(skip_stamping_for_backfill_data_)
{
    watermark = initWatermark(*params, logger);
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
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) { watermark->serialize(wb); });
}

void WatermarkTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType, ReadBuffer & rb) { watermark->deserialize(rb); });
}
}
}
