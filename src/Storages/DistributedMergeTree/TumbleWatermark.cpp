#include "TumbleWatermark.h"

#include <Core/Block.h>
#include <Functions/FunctionsStreamingWindow.h>
#include <common/ClockUtils.h>
#include <common/logger_useful.h>

namespace DB
{
TumbleWatermark::TumbleWatermark(WatermarkSettings && watermark_settings_, const String & partition_key_, Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), partition_key_, log_)
{
    HopTumbleBaseWatermark::init(window_interval);
    initTimezone(2);
}

Int64 TumbleWatermark::getWindowUpperBound(Int64 time_sec) const
{
    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        UInt32 w_start \
            = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_interval, *timezone); \
        return AddTime<IntervalKind::KIND>::execute(w_start, window_interval, *timezone); \
    }

        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}
}
