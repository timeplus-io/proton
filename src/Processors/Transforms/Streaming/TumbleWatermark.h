#pragma once

#include "HopTumbleBaseWatermark.h"

namespace DB
{
class TumbleWatermark : public HopTumbleBaseWatermark
{
public:
    explicit TumbleWatermark(WatermarkSettings && watermark_settings_, Poco::Logger * log);
    ~TumbleWatermark() override = default;

private:
    Int64 getWindowUpperBound(Int64 time_sec) const override;
};
}
