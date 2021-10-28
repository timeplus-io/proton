#include "WatermarkBlockInputStream.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsStreamingWindow.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTEmitQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/ClockUtils.h>
#include <common/DateLUT.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
    using ColumnDateTime32 = ColumnVector<UInt32>;

    WatermarkBlockInputStream::EmitMode mapMode(ASTEmitQuery::Mode mode)
    {
        switch (mode)
        {
            case ASTEmitQuery::Mode::PERIODIC:
                return WatermarkBlockInputStream::EmitMode::PERIODIC;
            case ASTEmitQuery::Mode::DELAY:
                return WatermarkBlockInputStream::EmitMode::DELAY;
            case ASTEmitQuery::Mode::WATERMARK:
                return WatermarkBlockInputStream::EmitMode::WATERMARK;
            case ASTEmitQuery::Mode::WATERMARK_WITH_DELAY:
                return WatermarkBlockInputStream::EmitMode::WATERMARK_WITH_DELAY;
        }
    }

    IntervalKind mapIntervalKind(const String & func_name)
    {
        if (func_name == "toIntervalSecond")
            return IntervalKind::Second;
        else if (func_name == "toIntervalMinute")
            return IntervalKind::Minute;
        else if (func_name == "toIntervalHour")
            return IntervalKind::Hour;
        else if (func_name == "toIntervalDay")
            return IntervalKind::Day;
        else if (func_name == "toIntervalWeek")
            return IntervalKind::Week;
        else if (func_name == "toIntervalMonth")
            return IntervalKind::Month;
        else if (func_name == "toIntervalQuarter")
            return IntervalKind::Quarter;
        else if (func_name == "toIntervalYear")
            return IntervalKind::Year;

        throw Exception("Invalid interval function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    Int64 addTime(Int64 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
    {
        switch (kind)
        {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        return AddTime<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
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

    void extractInterval(ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind)
    {
        assert(ast);

        kind = mapIntervalKind(ast->name);
        const auto * val = ast->children.front()->children.front()->as<ASTLiteral>();
        if (!val)
            throw Exception("Invalid interval argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (val->value.getType() == Field::Types::UInt64)
        {
            interval = val->value.safeGet<UInt64>();
        }
        else if (val->value.getType() == Field::Types::String)
        {
            interval = std::stoi(val->value.safeGet<String>());
        }
        else
            throw Exception("Invalid interval argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void extractWindow(const StreamingWindowDescription & desc, WatermarkBlockInputStream::EmitSettings & emit_settings)
    {
        assert(!desc.argument_names.empty());

        /// by default, the first argument is interval and we injected `_time` to streaming window function
        size_t win_arg_pos = 0;

        if (desc.argument_names.size() == desc.func_node->arguments->children.size())
        {
            /// user explicitly specifies a time column for streaming window function in the first argument
            win_arg_pos = 1;
        }

        emit_settings.time_col_name = desc.argument_names[0];
        emit_settings.time_col_is_datetime64 = isDateTime64(desc.argument_types[0]);
        if (emit_settings.time_col_is_datetime64)
        {
            emit_settings.scale = checkAndGetDataType<DataTypeDateTime64>(desc.argument_types[0].get())->getScale();
        }

        if (desc.func_node->name.find("HOP") != String::npos)
        {
            extractInterval(
                desc.func_node->arguments->children[1]->as<ASTFunction>(), emit_settings.hop_interval, emit_settings.hop_interval_kind);
            win_arg_pos += 1;
        }

        extractInterval(
            desc.func_node->arguments->children[win_arg_pos]->as<ASTFunction>(),
            emit_settings.window_interval,
            emit_settings.window_interval_kind);

        if (desc.func_node->arguments->children.size() > win_arg_pos + 1)
        {
            const auto * ast = desc.func_node->arguments->children[win_arg_pos + 1]->as<ASTLiteral>();
            if (!ast || ast->value.getType() != Field::Types::String)
                throw Exception("Invalid timezone", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            emit_settings.timezone = &DateLUT::instance(ast->value.safeGet<String>());
        }
    }

    void mergeEmitQuerySettings(const ASTPtr emit_query, WatermarkBlockInputStream::EmitSettings & emit_settings)
    {
        auto emit = emit_query->as<ASTEmitQuery>();
        emit_settings.streaming = emit->streaming;
        emit_settings.mode = mapMode(emit->mode);

        if (emit->interval)
        {
            extractInterval(emit->interval->as<ASTFunction>(), emit_settings.emit_query_interval, emit_settings.emit_query_interval_kind);
        }
    }

    WatermarkBlockInputStream::EmitSettings
    initWatermarkForAggrStreamWindowFunc(const ASTPtr emit_query, const StreamingWindowDescription & desc)
    {
        WatermarkBlockInputStream::EmitSettings emit_settings;
        emit_settings.func_name = desc.func_node->name;

        extractWindow(desc, emit_settings);

        if (emit_query)
        {
            mergeEmitQuerySettings(emit_query, emit_settings);
        }
        else
        {
            emit_settings.mode = WatermarkBlockInputStream::EmitMode::WATERMARK;
        }
        return emit_settings;
    }

    WatermarkBlockInputStream::EmitSettings initWatermarkForGlobalAggr(const ASTPtr emit_query)
    {
        WatermarkBlockInputStream::EmitSettings emit_settings;
        emit_settings.func_name = "Global";

        if (emit_query)
        {
            mergeEmitQuerySettings(emit_query, emit_settings);
        }
        else
        {
            emit_settings.mode = WatermarkBlockInputStream::EmitMode::PERIODIC;
            emit_settings.emit_query_interval = 2;
            emit_settings.emit_query_interval_kind = IntervalKind::Second;
        }
        return emit_settings;
    }

    template <typename TargetColumnType>
    void processBlock(
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

void WatermarkBlockInputStream::readPrefixImpl()
{
    if (emit_settings.mode == EmitMode::PERIODIC)
        watermark_ts = MonotonicMilliseconds::now();

    input->readPrefix();
}

WatermarkBlockInputStream::WatermarkBlockInputStream(
    BlockInputStreamPtr input_, const SelectQueryInfo & query_info, const String & partition_key_, Poco::Logger * log_)
    : input(input_), partition_key(partition_key_), log(log_)
{
    assert(input);
    initWatermark(query_info);
}

Block WatermarkBlockInputStream::readImpl()
{
    if (isCancelled())
    {
        return {};
    }

    /// FIXME, we assume it is `StreamingBlockInputStream` here
    /// which emits a header only block if there is no records from streaming store
    Block block = input->read();

    Int64 shrinked_max_event_ts = 0;
    if (block.rows())
    {
        const auto & time_col = block.getByName(emit_settings.time_col_name);
        /// We don't support column type changes in the middle of a streaming query
        if (emit_settings.time_col_is_datetime64)
        {
            /// const auto & datetime64_col = static_cast<const ColumnDateTime64 &>(*time_col.column);
            /// const ColumnDateTime64::Container & time_vec = datetime64_col.getData();
            /// auto scale = datetime64_col.getScale();
            Int64 scaled_watermark_ts = DecimalUtils::decimalFromComponents<DateTime64>(last_projected_watermark_ts, 0, emit_settings.scale);
            processBlock<ColumnDateTime64>(block, time_col, scaled_watermark_ts, max_event_ts, late_events);

            if (max_event_ts > 0)
            {
                shrinked_max_event_ts = DecimalUtils::getWholePart(DateTime64(max_event_ts), emit_settings.scale);
            }
        }
        else
        {
            processBlock<ColumnDateTime32>(block, time_col, last_projected_watermark_ts, max_event_ts, late_events);
            shrinked_max_event_ts = max_event_ts;
        }

        if (late_events > last_logged_late_events)
        {
            if (MonotonicSeconds::now() - last_logged_late_events_ts >= 5)
            {
                LOG_INFO(
                    log, "Found {} late events for data partition {}. Current last projected watermark={}",
                    late_events, partition_key, last_projected_watermark_ts);
                last_logged_late_events_ts = MonotonicSeconds::now();
                last_logged_late_events = late_events;
            }
        }
    }

    if (block.rows())
    {
        last_event_seen_ts = MonotonicSeconds::now();
        emitWatermark(block, shrinked_max_event_ts);
    }
//    else
//        handleIdleness(block);

    return block;
}

Int64 WatermarkBlockInputStream::getWindowUpperBound(Int64 time_sec)
{
    Int64 window_interval = emit_settings.window_interval;
    IntervalKind window_interval_kind = emit_settings.window_interval_kind;
    if (emit_settings.hop_interval != 0)
    {
        window_interval = emit_settings.hop_interval;
        window_interval_kind = emit_settings.hop_interval_kind;
    }

    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_interval, *emit_settings.timezone); \
        return AddTime<IntervalKind::KIND>::execute(w_start, window_interval, *emit_settings.timezone); \
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

void WatermarkBlockInputStream::handleIdleness(Block & block)
{
    switch (emit_settings.mode)
    {
        case EmitMode::PERIODIC:
        {
            auto now = MonotonicSeconds::now();
            auto next_watermark_ts
                = addTime(watermark_ts, emit_settings.emit_query_interval_kind, emit_settings.emit_query_interval, DateLUT::instance());

            if (now >= next_watermark_ts)
            {
                block.info.watermark = addTime(
                    last_projected_watermark_ts,
                    emit_settings.emit_query_interval_kind,
                    emit_settings.emit_query_interval,
                    *emit_settings.timezone);
                last_projected_watermark_ts = block.info.watermark;
                watermark_ts = now;
            }
            break;
        }
        case EmitMode::DELAY:
        {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case EmitMode::WATERMARK:
            /// FALLTHROUGH
        case EmitMode::WATERMARK_WITH_DELAY:
        {
            if (watermark_ts != 0)
            {
                auto next_watermark_ts
                    = addTime(last_event_seen_ts, emit_settings.window_interval_kind, emit_settings.window_interval_kind, DateLUT::instance());

                if (MonotonicSeconds::now() > next_watermark_ts)
                {
                    /// idle source
                    block.info.watermark = watermark_ts;
                    last_projected_watermark_ts = watermark_ts;
                    last_event_seen_ts = next_watermark_ts;

                    /// force watermark progressing
                    if (emit_settings.hop_interval != 0)
                        watermark_ts
                            = addTime(watermark_ts, emit_settings.hop_interval_kind, emit_settings.hop_interval, *emit_settings.timezone);
                    else
                        watermark_ts = addTime(
                            watermark_ts, emit_settings.window_interval_kind, emit_settings.window_interval, *emit_settings.timezone);
                }
            }
            break;
        }
    }
}

void WatermarkBlockInputStream::emitWatermark(Block & block, Int64 shrinked_max_event_ts)
{
    switch (emit_settings.mode)
    {
        case EmitMode::PERIODIC:
        {
            auto now = MonotonicSeconds::now();
            auto next_watermark_ts
                = addTime(watermark_ts, emit_settings.emit_query_interval_kind, emit_settings.emit_query_interval, DateLUT::instance());
            if (now >= next_watermark_ts)
            {
                block.info.watermark = shrinked_max_event_ts;
                last_projected_watermark_ts = shrinked_max_event_ts;
                watermark_ts = now;
            }
            break;
        }
        case EmitMode::DELAY:
        {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case EmitMode::WATERMARK:
        {
            if (watermark_ts != 0)
            {
                while (watermark_ts <= shrinked_max_event_ts)
                {
                    /// emit the max watermark
                    block.info.watermark = watermark_ts;
                    last_projected_watermark_ts = watermark_ts;
                    if (emit_settings.hop_interval != 0)
                        watermark_ts
                            = addTime(watermark_ts, emit_settings.hop_interval_kind, emit_settings.hop_interval, *emit_settings.timezone);
                    else
                        watermark_ts = addTime(
                            watermark_ts, emit_settings.window_interval_kind, emit_settings.window_interval, *emit_settings.timezone);
                }
            }
            else
            {
                if (shrinked_max_event_ts > 0)
                    watermark_ts = getWindowUpperBound(shrinked_max_event_ts - 1);
            }
            break;
        }
        case EmitMode::WATERMARK_WITH_DELAY:
        {
            if (watermark_ts != 0)
            {
                UInt32 watermark_ts_bias = addTime(
                    watermark_ts, emit_settings.emit_query_interval_kind, emit_settings.emit_query_interval, *emit_settings.timezone);

                while (watermark_ts_bias <= shrinked_max_event_ts)
                {
                    block.info.watermark = watermark_ts;
                    last_projected_watermark_ts = watermark_ts;
                    if (emit_settings.hop_interval != 0)
                    {
                        watermark_ts
                            = addTime(watermark_ts, emit_settings.hop_interval_kind, emit_settings.hop_interval, *emit_settings.timezone);
                        watermark_ts_bias
                            = addTime(watermark_ts, emit_settings.hop_interval_kind, emit_settings.hop_interval, *emit_settings.timezone);
                    }
                    else
                    {
                        watermark_ts = addTime(
                            watermark_ts, emit_settings.window_interval_kind, emit_settings.window_interval, *emit_settings.timezone);
                        watermark_ts_bias = addTime(
                            watermark_ts, emit_settings.window_interval_kind, emit_settings.window_interval, *emit_settings.timezone);
                    }
                }
            }
            else
            {
                if (shrinked_max_event_ts > 0)
                    watermark_ts = getWindowUpperBound(shrinked_max_event_ts - 1);
            }
            break;
        }
    }
}

void WatermarkBlockInputStream::initWatermark(const SelectQueryInfo & query_info)
{
    if (query_info.syntax_analyzer_result->aggregates.empty())
    {
        /// if there is no aggregation, we don't need project watermark
        /// we don't support `order by` a stream
        return;
    }

    const auto * select_query = query_info.query->as<ASTSelectQuery>();
    assert(select_query);

    auto emit_query = select_query->emit();

    if (query_info.streaming_win_func)
    {
        /// has aggregates + streaming window function
        /// FIXME, is the aggregate over streaming window function ?
        emit_settings = initWatermarkForAggrStreamWindowFunc(emit_query, *query_info.streaming_win_func);
    }
    else
    {
        /// aggregate but not over streaming window function
        emit_settings = initWatermarkForGlobalAggr(emit_query);
    }

    if (!emit_settings.timezone)
    {
        emit_settings.timezone = &DateLUT::instance();
    }
}

void WatermarkBlockInputStream::cancel(bool kill)
{
    input->cancel(kill);
    IBlockInputStream::cancel(kill);
}
}
