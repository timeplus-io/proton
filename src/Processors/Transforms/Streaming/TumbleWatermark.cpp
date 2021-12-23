#include "TumbleWatermark.h"

#include <base/ClockUtils.h>
#include <base/logger_useful.h>

namespace DB
{
TumbleWatermark::TumbleWatermark(WatermarkSettings && watermark_settings_, bool proc_time_, Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), proc_time_, log_)
{
    HopTumbleBaseWatermark::init(window_interval);
    initTimezone(2);
}
}
