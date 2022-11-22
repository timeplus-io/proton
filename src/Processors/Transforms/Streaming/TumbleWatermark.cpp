#include "TumbleWatermark.h"

#include <base/ClockUtils.h>
#include <base/logger_useful.h>

namespace DB
{
namespace Streaming
{
TumbleWatermark::TumbleWatermark(WatermarkSettings && watermark_settings_, size_t time_col_position_, bool proc_time_, Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), time_col_position_, proc_time_, log_)
{
    init(window_interval);
    initTimezone(2);
}
}
}
