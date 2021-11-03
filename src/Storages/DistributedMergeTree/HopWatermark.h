#pragma once

#include "HopTumbleBaseWatermark.h"

namespace DB
{
class HopWatermark final : public HopTumbleBaseWatermark
{
public:
    explicit HopWatermark(WatermarkSettings && watermark_settings_, const String & partition_key_, Poco::Logger * log_);
    ~HopWatermark() override = default;

private:
    /// EMIT STREAM AFTER WATERMARK
    void processWatermarkWithDelay(Block & block, Int64 max_event_ts_secs) override;

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    void processWatermark(Block & block, Int64 max_event_ts_secs) override;

    void handleIdlenessWatermark(Block & block) override;

    void handleIdlenessWatermarkWithDelay(Block & block) override;

private:
    Int64 getWindowUpperBound(Int64 time_sec) const;

private:
    Int64 hop_interval = 0;
};
}
