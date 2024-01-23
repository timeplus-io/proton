#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

#include <Processors/ISimpleTransform.h>

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
    WatermarkTransform(const Block & header, WatermarkStamperParamsPtr params_, bool skip_stamping_for_backfill_data_, Poco::Logger * log);

    ~WatermarkTransform() override = default;

    String getName() const override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

private:
    WatermarkStamperParamsPtr params;
    SERDE WatermarkStamperPtr watermark;

    bool skip_stamping_for_backfill_data;
    bool mute_watermark = false;
};
}
}
