#include "HopTumbleBaseWatermark.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <common/ClockUtils.h>
#include <common/DateLUT.h>
#include <common/logger_useful.h>

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

HopTumbleBaseWatermark::HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, const String & partition_key_, Poco::Logger * log_)
    : Watermark(std::move(watermark_settings_), partition_key_, log_)
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

    /// By default, the first argument is interval and we injected `_time` to streaming window function
    if (desc.argument_names.size() == desc.func_node->arguments->children.size())
    {
        /// Tricky part, desc.argument_names can have one more element then desc.func_node->arguments->children
        /// since we does additional processing to stuff in `_time` when user omits the timestamp parameter. FIXME
        /// User explicitly specifies a time column for streaming window function in the first argument
        win_arg_pos = 1;
    }

    extractInterval(
        desc.func_node->arguments->children[win_arg_pos]->as<ASTFunction>(), interval, window_interval_kind);
}

void HopTumbleBaseWatermark::initTimezone()
{
    const auto & desc = *watermark_settings.window_desc;
    if (desc.func_node->arguments->children.size() > win_arg_pos + 1)
    {
        const auto * ast = desc.func_node->arguments->children[win_arg_pos + 1]->as<ASTLiteral>();
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
    Int64 max_event_ts_secs = 0;
    const auto & time_col = block.getByName(time_col_name);

    /// We don't support column type changes in the middle of a streaming query
    if (time_col_is_datetime64)
    {
        /// const auto & datetime64_col = static_cast<const ColumnDateTime64 &>(*time_col.column);
        /// const ColumnDateTime64::Container & time_vec = datetime64_col.getData();
        /// auto scale = datetime64_col.getScale();
        Int64 scaled_watermark_ts = DecimalUtils::decimalFromComponents<DateTime64>(last_projected_watermark_ts, 0, scale);
        doProcessBlock<ColumnDateTime64>(block, time_col, scaled_watermark_ts, max_event_ts, late_events);

        if (max_event_ts > 0)
        {
            max_event_ts_secs = DecimalUtils::getWholePart(DateTime64(max_event_ts), scale);
        }
    }
    else
    {
        doProcessBlock<ColumnDateTime32>(block, time_col, last_projected_watermark_ts, max_event_ts, late_events);
        max_event_ts_secs = max_event_ts;
    }

    if (late_events > last_logged_late_events)
    {
        if (MonotonicSeconds::now() - last_logged_late_events_ts >= 5)
        {
            LOG_INFO(
                log,
                "Found {} late events for data partition {}. Current last projected watermark={}",
                late_events,
                partition_key,
                last_projected_watermark_ts);
            last_logged_late_events_ts = MonotonicSeconds::now();
            last_logged_late_events = late_events;
        }
    }

    if (block.rows())
    {
        last_event_seen_ts = UTCSeconds::now();
        assignWatermark(block, max_event_ts_secs);
    }
}
}
