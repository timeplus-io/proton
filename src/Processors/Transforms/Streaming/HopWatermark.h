#pragma once

#include "HopTumbleBaseWatermark.h"

namespace DB
{
class HopWatermark final : public HopTumbleBaseWatermark
{
public:
    HopWatermark(WatermarkSettings && watermark_settings_, Poco::Logger * log_);
    ~HopWatermark() override = default;

private:
    Int64 getProgressingInterval() const override { return hop_interval; }

private:
    Int64 hop_interval = 0;
};
}
