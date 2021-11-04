#include "Watermark.h"

#include <Core/Block.h>
#include <Functions/FunctionsStreamingWindow.h>
#include <Interpreters/StreamingFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTEmitQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <common/ClockUtils.h>
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

    WatermarkSettings::EmitMode mapMode(ASTEmitQuery::Mode mode)
    {
        switch (mode)
        {
            case ASTEmitQuery::Mode::NONE:
                return WatermarkSettings::EmitMode::NONE;
            case ASTEmitQuery::Mode::TAIL:
                return WatermarkSettings::EmitMode::TAIL;
            case ASTEmitQuery::Mode::PERIODIC:
                return WatermarkSettings::EmitMode::PERIODIC;
            case ASTEmitQuery::Mode::DELAY:
                return WatermarkSettings::EmitMode::DELAY;
            case ASTEmitQuery::Mode::WATERMARK:
                return WatermarkSettings::EmitMode::WATERMARK;
            case ASTEmitQuery::Mode::WATERMARK_WITH_DELAY:
                return WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY;
        }
    }
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

namespace
{
    void mergeEmitQuerySettings(const ASTPtr & emit_query, WatermarkSettings & watermark_settings)
    {
        if (!emit_query)
        {
            return;
        }

        auto emit = emit_query->as<ASTEmitQuery>();
        assert(emit);

        watermark_settings.streaming = emit->streaming;
        watermark_settings.mode = mapMode(emit->mode);

        if (emit->interval)
        {
            extractInterval(
                emit->interval->as<ASTFunction>(), watermark_settings.emit_query_interval, watermark_settings.emit_query_interval_kind);
        }
    }
}

WatermarkSettings::WatermarkSettings(const SelectQueryInfo & query_info, StreamingFunctionDescriptionPtr desc)
{
    window_desc = std::move(desc);

    const auto * select_query = query_info.query->as<ASTSelectQuery>();
    assert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    if (query_info.syntax_analyzer_result->aggregates.empty())
    {
        /// if there is no aggregation, we don't need project watermark
        /// we don't support `order by` a stream
        mode = EmitMode::TAIL;
    }
    else
    {
        if (window_desc)
        {
            func_name = window_desc->func_ast->as<ASTFunction>()->name;

            if (mode == WatermarkSettings::EmitMode::NONE)
                mode = WatermarkSettings::EmitMode::WATERMARK;
        }
        else
        {
            /// Aggregate but not over streaming window function
            initWatermarkForGlobalAggr();
        }
    }
}

void WatermarkSettings::initWatermarkForGlobalAggr()
{
    func_name = "GlobalAggr";
    global_aggr = true;

    if (mode == EmitMode::NONE)
    {
        /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
        mode = EmitMode::PERIODIC;
        emit_query_interval = 2;
        emit_query_interval_kind = IntervalKind::Second;
    }
}

void Watermark::preProcess()
{
    if (watermark_settings.mode == WatermarkSettings::EmitMode::PERIODIC)
        watermark_ts = UTCSeconds::now();
}

void Watermark::process(Block & block)
{
    if (watermark_settings.mode == WatermarkSettings::EmitMode::TAIL)
        return;

    if (watermark_settings.global_aggr)
    {
        /// global aggr emitted by using wall clock time of the current server
        last_event_seen_ts = UTCSeconds::now();
        assignWatermark(block, UTCSeconds::now());
        return;
    }

    if (block.rows())
        doProcess(block);

    /// If after filtering, block is empty, we handle idleness
    //    if (!block.rows())
    //        handleIdleness(block);
}

void Watermark::assignWatermark(Block & block, Int64 max_event_ts_secs)
{
    switch (watermark_settings.mode)
    {
        case WatermarkSettings::EmitMode::NONE:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::TAIL:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::PERIODIC: {
            auto now = UTCSeconds::now();
            auto next_watermark_ts = addTime(
                watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, DateLUT::instance());
            if (now >= next_watermark_ts)
            {
                block.info.watermark = max_event_ts_secs;
                last_projected_watermark_ts = max_event_ts_secs;
                watermark_ts = now;
                LOG_INFO(log, "Periodic time={}, rows={}", block.info.watermark, block.rows());
            }
            break;
        }
        case WatermarkSettings::EmitMode::DELAY: {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case WatermarkSettings::EmitMode::WATERMARK:
            processWatermark(block, max_event_ts_secs);
            break;

        case WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY:
            processWatermarkWithDelay(block, max_event_ts_secs);
            break;
    }
}

void Watermark::handleIdleness(Block & block)
{
    switch (watermark_settings.mode)
    {
        case WatermarkSettings::EmitMode::NONE:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::TAIL:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::PERIODIC: {
            auto now = UTCSeconds::now();
            auto next_watermark_ts = addTime(
                watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, DateLUT::instance());

            if (now >= next_watermark_ts)
            {
                block.info.watermark = addTime(
                    last_projected_watermark_ts,
                    watermark_settings.emit_query_interval_kind,
                    watermark_settings.emit_query_interval,
                    DateLUT::instance());
                last_projected_watermark_ts = block.info.watermark;
                watermark_ts = now;
            }
            break;
        }
        case WatermarkSettings::EmitMode::DELAY: {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case WatermarkSettings::EmitMode::WATERMARK:
            handleIdlenessWatermark(block);
            break;
        case WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY:
            handleIdlenessWatermarkWithDelay(block);
            break;
    }
}
}
