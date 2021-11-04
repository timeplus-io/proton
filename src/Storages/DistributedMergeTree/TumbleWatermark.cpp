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

void TumbleWatermark::processWatermarkWithDelay(Block & block, Int64 max_event_ts_secs)
{
    if (watermark_ts != 0)
    {
        UInt32 watermark_ts_bias = addTime(
            watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);

        while (watermark_ts_bias <= max_event_ts_secs)
        {
            block.info.watermark = watermark_ts;
            last_projected_watermark_ts = watermark_ts;
            watermark_ts = addTime(watermark_ts, window_interval_kind, window_interval, *timezone);
            watermark_ts_bias = addTime(watermark_ts, window_interval_kind, window_interval, *timezone);
        }
        LOG_INFO(log, "Emitted watermark={}", block.info.watermark);
    }
    else
    {
        if (max_event_ts_secs > 0)
            watermark_ts = getWindowUpperBound(max_event_ts_secs);
    }
}

void TumbleWatermark::processWatermark(Block & block, Int64 max_event_ts_secs)
{
    if (watermark_ts != 0)
    {
        while (watermark_ts <= max_event_ts_secs)
        {
            /// emit the max watermark
            block.info.watermark = watermark_ts;
            last_projected_watermark_ts = watermark_ts;
            watermark_ts = addTime(watermark_ts, window_interval_kind, window_interval, *timezone);
        }
        LOG_INFO(log, "Emitted watermark={}", block.info.watermark);
    }
    else
    {
        if (max_event_ts_secs > 0)
            watermark_ts = getWindowUpperBound(max_event_ts_secs);
    }
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

void TumbleWatermark::handleIdlenessWatermark(Block & block)
{
    if (watermark_ts != 0)
    {
        auto next_watermark_ts
            = addTime(last_event_seen_ts, window_interval_kind, window_interval_kind, DateLUT::instance());

        if (UTCSeconds::now() > next_watermark_ts)
        {
            /// idle source
            block.info.watermark = watermark_ts;
            last_projected_watermark_ts = watermark_ts;
            last_event_seen_ts = next_watermark_ts;

            /// Force watermark progressing
            watermark_ts = addTime(watermark_ts, window_interval_kind, window_interval, *timezone);
        }
    }
}

void TumbleWatermark::handleIdlenessWatermarkWithDelay(Block & block)
{
    handleIdlenessWatermark(block);
}
}
