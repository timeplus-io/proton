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
WatermarkStamperPtr initWatermark(WatermarkStamperParams params, Poco::Logger * log)
{
    assert(params.mode != WatermarkStamperParams::EmitMode::NONE);
    if (params.window_params)
    {
        switch (params.window_params->type)
        {
            case WindowType::TUMBLE:
                return std::make_unique<TumbleWatermarkStamper>(std::move(params), log);
            case WindowType::HOP:
                return std::make_unique<HopWatermarkStamper>(std::move(params), log);
            case WindowType::SESSION:
                return std::make_unique<SessionWatermarkStamper>(std::move(params), log);
            default:
                break;
        }
    }
    return std::make_unique<WatermarkStamper>(std::move(params), log);
}
}

WatermarkTransform::WatermarkTransform(const Block & header, WatermarkStamperParams params, Poco::Logger * log)
    : ISimpleTransform(header, header, false, ProcessorID::WatermarkTransformID)
{
    watermark = initWatermark(std::move(params), log);
    assert(watermark);
    watermark->preProcess(header);
}

void WatermarkTransform::transform(Chunk & chunk)
{
    if (auto ckpt_ctx = chunk.getCheckpointContext(); ckpt_ctx)
    {
        checkpoint(std::move(ckpt_ctx));
        /// Checkpoint barrier is always standalone
        /// Directly return automatically propagate the checkpoint barrier to down stream
        return;
    }

    if (!chunk.avoidWatermark())
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
