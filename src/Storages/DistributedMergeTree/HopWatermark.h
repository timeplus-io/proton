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
    Int64 getProgressingInterval() const override { return hop_interval; }
    Int64 getWindowUpperBound(Int64 time_sec) const override;

private:
    Int64 hop_interval = 0;
};
}
