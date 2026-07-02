#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

#include <Processors/ISimpleTransform.h>
#include <Common/Logger.h>

namespace DB
{
/**
 * WatermarkTransform projects watermark according to watermark strategies
 * by observing the events in its input.
 */

namespace Streaming
{
class WatermarkTransform final : public ISimpleTransform
{
public:
    WatermarkTransform(const Block & header, EmitParamsPtr params_, bool skip_stamping_for_backfill_data_);

    ~WatermarkTransform() override = default;

    String getName() const override { return watermark->getName() + "Transform"; }

    bool hasState() const override { return true; }
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

private:
    EmitParamsPtr params;
    SERDE WatermarkStamperPtr watermark;

    bool skip_stamping_for_backfill_data;
    bool mute_watermark = false;

    /// Not serialized: a consecutive pair cannot straddle a checkpoint barrier.
    bool prev_consecutive = false;

    LoggerPtr logger;
};
}
}
