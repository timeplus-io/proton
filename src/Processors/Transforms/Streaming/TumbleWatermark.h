#pragma once

#include "HopTumbleBaseWatermark.h"

namespace DB
{
class TumbleWatermark : public HopTumbleBaseWatermark
{
public:
    explicit TumbleWatermark(WatermarkSettings && watermark_settings_, bool proc_time_, Poco::Logger * log);
    ~TumbleWatermark() override = default;
};
}
