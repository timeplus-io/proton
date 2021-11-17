#include "HopTumbleBaseWatermark.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsStreamingWindow.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <base/ClockUtils.h>
#include <base/DateLUT.h>
#include <base/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
    using ColumnDateTime32 = ColumnVector<UInt32>;

    template <typename TargetColumnType>
    void doProcessBlock(
        Block & block, const ColumnWithTypeAndName & time_col, Int64 last_project_watermark_ts, Int64 & max_event_ts, UInt64 & late_events)
    {
        /// FIXME, use simple FilterTransform to do this ?
        const typename TargetColumnType::Container & time_vec = checkAndGetColumn<TargetColumnType>(time_col.column.get())->getData();
        IColumn::Filter filt(time_vec.size(), 1);

        UInt64 late_events_in_block = 0;
        for (size_t i = 0; i < time_vec.size(); ++i)
        {
            if (time_vec[i] > max_event_ts)
                max_event_ts = time_vec[i];
            else if (time_vec[i] <= last_project_watermark_ts)
            {
                filt[i] = 0;
                late_events_in_block += 1;
            }
        }

        if (late_events_in_block > 0)
        {
            late_events += late_events_in_block;
            for (auto & col_with_name_type : block)
            {
                col_with_name_type.column = col_with_name_type.column->filter(filt, time_vec.size() - late_events_in_block);
            }
        }
    }
}

HopTumbleBaseWatermark::HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, Poco::Logger * log_)
    : Watermark(std::move(watermark_settings_), log_)
{
    if (watermark_settings.mode == WatermarkSettings::EmitMode::NONE)
        watermark_settings.mode = WatermarkSettings::EmitMode::WATERMARK;
}

void HopTumbleBaseWatermark::init(Int64 & interval)
{
    assert(watermark_settings.window_desc);
    assert(!watermark_settings.window_desc->argument_names.empty());

    const auto & desc = *watermark_settings.window_desc;
    time_col_name = desc.argument_names[0];
    time_col_is_datetime64 = isDateTime64(desc.argument_types[0]);

    if (time_col_is_datetime64)
    {
        scale = checkAndGetDataType<DataTypeDateTime64>(desc.argument_types[0].get())->getScale();
    }

    auto * func_ast = desc.func_ast->as<ASTFunction>();
    extractInterval(func_ast->arguments->children[1]->as<ASTFunction>(), interval, window_interval_kind);
}

void HopTumbleBaseWatermark::initTimezone(size_t timezone_pos)
{
    const auto & desc = *watermark_settings.window_desc;
    auto * func_ast = desc.func_ast->as<ASTFunction>();
    if (func_ast->arguments->children.size() == timezone_pos + 1)
    {
        const auto * ast = func_ast->arguments->children[timezone_pos]->as<ASTLiteral>();
        if (!ast || ast->value.getType() != Field::Types::String)
            throw Exception("Invalid timezone", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        timezone = &DateLUT::instance(ast->value.safeGet<String>());
    }
    else
    {
        timezone = &DateLUT::instance();
    }
}

void HopTumbleBaseWatermark::doProcess(Block & block)
{
    const auto & time_col = block.getByName(time_col_name);

    /// We don't support column type changes in the middle of a streaming query
    if (time_col_is_datetime64)
    {
        /// const auto & datetime64_col = static_cast<const ColumnDateTime64 &>(*time_col.column);
        /// const ColumnDateTime64::Container & time_vec = datetime64_col.getData();
        /// auto scale = datetime64_col.getScale();
        doProcessBlock<ColumnDateTime64>(block, time_col, last_projected_watermark_ts, max_event_ts, late_events);
    }
    else
    {
        doProcessBlock<ColumnDateTime32>(block, time_col, last_projected_watermark_ts, max_event_ts, late_events);
    }

    if (late_events > last_logged_late_events)
    {
        if (MonotonicSeconds::now() - last_logged_late_events_ts >= 5)
        {
            LOG_INFO(log, "Found {} late events for data. Current last projected watermark={}", late_events, last_projected_watermark_ts);
            last_logged_late_events_ts = MonotonicSeconds::now();
            last_logged_late_events = late_events;
        }
    }

    if (block.rows())
    {
        last_event_seen_ts = UTCSeconds::now();
        assignWatermark(block);
    }
}

void HopTumbleBaseWatermark::processWatermark(Block & block)
{
    if (watermark_ts != 0)
    {
        auto interval = getProgressingInterval();
        /// FIXME, use multiply for optimization instead of loop
        while (watermark_ts <= max_event_ts)
        {
            /// emit the max watermark
            last_projected_watermark_ts = watermark_ts;
            if (scale != 0)
                watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            else
                watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
        }

        block.info.watermark = last_projected_watermark_ts;
        if (last_projected_watermark_ts > 0)
        {
            if (scale != 0)
                block.info.watermark_lower_bound = addTimeWithAutoScale(last_projected_watermark_ts, window_interval_kind, -1 * window_interval);
            else
                block.info.watermark_lower_bound = addTime(last_projected_watermark_ts, window_interval_kind, -1 * window_interval, *timezone);

            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", block.info.watermark, block.info.watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow();
}

void HopTumbleBaseWatermark::processWatermarkWithDelay(Block & block)
{
    if (watermark_ts != 0)
    {
        Int64 watermark_ts_bias = 0;
        if (scale != 0)
            watermark_ts_bias
                = addTimeWithAutoScale(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval);
        else
            watermark_ts_bias
                = addTime(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);

        auto interval = getProgressingInterval();
        while (watermark_ts_bias <= max_event_ts)
        {
            last_projected_watermark_ts = watermark_ts;

            if (scale != 0)
            {
                watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
                watermark_ts_bias = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            }
            else
            {
                watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
                watermark_ts_bias = addTime(watermark_ts, window_interval_kind, interval, *timezone);
            }
        }

        block.info.watermark = last_projected_watermark_ts;
        if (last_projected_watermark_ts > 0)
        {
            if (scale != 0)
                block.info.watermark_lower_bound = addTimeWithAutoScale(last_projected_watermark_ts, window_interval_kind, -1 * window_interval);
            else
                block.info.watermark_lower_bound = addTime(last_projected_watermark_ts, window_interval_kind, -1 * window_interval, *timezone);

            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", block.info.watermark, block.info.watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow();
}

void HopTumbleBaseWatermark::handleIdlenessWatermark(Block & block)
{
    /// FIXME, this is not a complete implementation
    if (watermark_ts != 0)
    {
        auto interval = getProgressingInterval();
        auto next_watermark_ts = addTime(last_event_seen_ts, window_interval_kind, interval, DateLUT::instance());

        if (UTCSeconds::now() > next_watermark_ts)
        {
            block.info.watermark = watermark_ts;
            last_projected_watermark_ts = watermark_ts;

            last_event_seen_ts = next_watermark_ts;

            /// Force watermark progressing
            if (scale != 0)
            {
                block.info.watermark_lower_bound = addTimeWithAutoScale(block.info.watermark, window_interval_kind, -1 * window_interval);
                watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            }
            else
            {
                block.info.watermark_lower_bound = addTime(block.info.watermark, window_interval_kind, -1 * window_interval, *timezone);
                watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
            }
        }
    }
}

void HopTumbleBaseWatermark::handleIdlenessWatermarkWithDelay(Block & block)
{
    handleIdlenessWatermark(block);
}

std::pair<Int64, Int64> HopTumbleBaseWatermark::initFirstWindow() const
{
    if (max_event_ts > 0)
    {
        if (scale != 0)
        {
            assert(time_col_is_datetime64);
            /// Scale in and scale out, if it is a DateTime64 as addTime doesn't supports scale yet. FIXME
            auto scaled_watermark_ts_secs = DecimalUtils::getWholePart(DateTime64(max_event_ts), scale);
            auto window = getWindow(scaled_watermark_ts_secs);
            return {
                DecimalUtils::decimalFromComponents<DateTime64>(window.first, 0, scale),
                DecimalUtils::decimalFromComponents<DateTime64>(window.second, 0, scale)};
        }
        else
            return getWindow(max_event_ts);
    }

    return {watermark_ts, watermark_ts};
}

ALWAYS_INLINE Int64 HopTumbleBaseWatermark::addTimeWithAutoScale(Int64 datetime64, IntervalKind::Kind interval_kind, Int64 interval)
{
    assert(time_col_is_datetime64);
    assert(scale != 0);

    /// Scale in and scale out, if it is a DateTime64 as addTime doesn't supports scale yet. FIXME
    auto scaled_secs = DecimalUtils::getWholePart(DateTime64(datetime64), scale);
    scaled_secs = addTime(scaled_secs, interval_kind, interval, *timezone);
    return DecimalUtils::decimalFromComponents<DateTime64>(scaled_secs, 0, scale);
}

ALWAYS_INLINE std::pair<Int64, Int64> HopTumbleBaseWatermark::getWindow(Int64 time_sec) const
{
    auto interval = getProgressingInterval();
    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        auto w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, interval, *timezone); \
        return {w_start, AddTime<IntervalKind::KIND>::execute(w_start, interval, *timezone)}; \
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
