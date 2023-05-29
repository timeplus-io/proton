#include "WatermarkStamper.h"

#include <Functions/Streaming/FunctionsStreamingWindow.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Processors/Chunk.h>
#include <Storages/SelectQueryInfo.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include <Common/VersionRevision.h>
#include <Common/logger_useful.h>

#include <magic_enum.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
namespace
{
void mergeEmitQuerySettings(const ASTPtr & emit_query, WatermarkStamperParams & params)
{
    if (!emit_query)
    {
        return;
    }

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
    {
        if (emit->after_watermark)
            throw Exception("Streaming doesn't support having both any watermark and periodic emit", ErrorCodes::INCORRECT_QUERY);

        if (emit->delay_interval)
            throw Exception("Streaming doesn't support having both delay and periodic emit", ErrorCodes::INCORRECT_QUERY);

        extractInterval(emit->periodic_interval->as<ASTFunction>(), params.periodic_interval, params.periodic_interval_kind);

        params.mode = WatermarkStamperParams::EmitMode::PERIODIC;
    }
    else if (emit->after_watermark)
    {
        if (!params.window_params)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "Watermark emit is only supported in streaming queries with streaming window function");

        params.mode = params.window_params->type == WindowType::SESSION ? WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW
                                                                        : WatermarkStamperParams::EmitMode::WATERMARK;
    }
    else
        params.mode = WatermarkStamperParams::EmitMode::NONE;

    if (emit->timeout_interval)
        extractInterval(emit->timeout_interval->as<ASTFunction>(), params.timeout_interval, params.timeout_interval_kind);

    if (emit->delay_interval)
        extractInterval(emit->delay_interval->as<ASTFunction>(), params.delay_interval, params.delay_interval_kind);
}
}

WatermarkStamperParams::WatermarkStamperParams(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, WindowParamsPtr window_params_)
    : window_params(std::move(window_params_))
{
    const auto * select_query = query->as<ASTSelectQuery>();
    assert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    if (syntax_analyzer_result->aggregates.empty() && !syntax_analyzer_result->has_group_by)
    {
        /// For streaming non-aggregation query
        if (mode != EmitMode::TAIL && mode != EmitMode::NONE)
            throw Exception("Streaming tail mode doesn't support any watermark or periodic emit", ErrorCodes::INCORRECT_QUERY);

        /// Set default emit mode
        if (mode == EmitMode::NONE)
            mode = EmitMode::TAIL;
    }
    else
    {
        /// For streaming aggregation query
        if (mode == EmitMode::TAIL)
            throw Exception("Streaming aggregation doesn't support tail emit", ErrorCodes::INCORRECT_QUERY);

        if (mode == EmitMode::PERIODIC && window_params)
            throw Exception("Streaming window aggregation doesn't support periodic emit", ErrorCodes::INCORRECT_QUERY);

        /// Set default emit mode
        if (mode == EmitMode::NONE)
        {
            if (window_params)
            {
                mode = window_params->type == WindowType::SESSION ? EmitMode::WATERMARK_PER_ROW : EmitMode::WATERMARK;
            }
            else
            {
                /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
                mode = EmitMode::PERIODIC;
                periodic_interval = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first;
                periodic_interval_kind = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second;
            }
        }
    }
}

void WatermarkStamper::preProcess(const Block & header)
{
    switch (params.mode)
    {
        case WatermarkStamperParams::EmitMode::PERIODIC: {
            if (params.periodic_interval_kind > IntervalKind::Day)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The maximum interval kind of streaming periodic emit policy is day.");

            /// In fact, periodic emit has the same logic as timeout emit, so we internally switch to timeout path
            params.timeout_interval = params.periodic_interval;
            params.timeout_interval_kind = params.periodic_interval_kind;
            break;
        }
        case WatermarkStamperParams::EmitMode::WATERMARK:
            [[fallthrough]];
        case WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW: {
            assert(params.window_params);
            if (!header.has(params.window_params->desc->argument_names[0]))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The event time columns not found");

            time_col_pos = header.getPositionByName(params.window_params->desc->argument_names[0]);
            break;
        }
        default:
            break;
    }

    /// WITH EMIT TIMEOUT
    if (params.timeout_interval != 0)
    {
        if (params.timeout_interval_kind > IntervalKind::Day)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The maximum interval kind of emit timeout is day.");

        if (params.timeout_interval_kind != IntervalKind::Nanosecond)
        {
            params.timeout_interval = BaseScaleInterval::toBaseScale(params.timeout_interval, params.timeout_interval_kind)
                                          .toIntervalKind(IntervalKind::Nanosecond);
            params.timeout_interval_kind = IntervalKind::Nanosecond;
        }

        next_emit_timeout_ts = MonotonicNanoseconds::now() + params.timeout_interval;
    }
}

void WatermarkStamper::process(Chunk & chunk)
{
    switch (params.mode)
    {
        case WatermarkStamperParams::EmitMode::WATERMARK: {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                processWatermark<ColumnDateTime64, false>(chunk);
            else
                processWatermark<ColumnDateTime, false>(chunk);
            break;
        }
        case WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW: {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                processWatermark<ColumnDateTime64, true>(chunk);
            else
                processWatermark<ColumnDateTime, true>(chunk);
            break;
        }
        default:
            break;
    }

    processTimeout(chunk);
    logLateEvents();
}

void WatermarkStamper::processTimeout(Chunk & chunk)
{
    if (next_emit_timeout_ts == 0)
        return;

    /// FIXME: use a Timer.
    auto now = MonotonicNanoseconds::now();

    /// Update next timeout ts if emitted
    if (chunk.hasWatermark())
    {
        next_emit_timeout_ts = now + params.timeout_interval;
        return;
    }

    if (now < next_emit_timeout_ts)
        return;

    watermark_ts = max_event_ts + 1;
    next_emit_timeout_ts = now + params.timeout_interval;

    auto chunk_ctx = chunk.getOrCreateChunkContext();
    if (params.mode == WatermarkStamperParams::EmitMode::PERIODIC)
    {
        chunk_ctx->setWatermark(now);
        LOG_DEBUG(log, "Periodic emit time={}, rows={}", now, chunk.getNumRows());
    }
    else
    {
        chunk_ctx->setWatermark(TIMEOUT_WATERMARK);
        LOG_DEBUG(log, "Timeout emit time={}, rows={}", now, chunk.getNumRows());
    }

    chunk.setChunkContext(std::move(chunk_ctx));
}

void WatermarkStamper::logLateEvents()
{
    if (late_events > last_logged_late_events)
    {
        if (MonotonicSeconds::now() - last_logged_late_events_ts >= LOG_LATE_EVENTS_INTERVAL_SECONDS)
        {
            LOG_INFO(log, "Found {} late events for data. Last projected watermark={}", late_events, watermark_ts);
            last_logged_late_events_ts = MonotonicSeconds::now();
            last_logged_late_events = late_events;
        }
    }
}

template <typename TimeColumnType, bool apply_watermark_per_row>
void WatermarkStamper::processWatermark(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    assert(params.window_params);

    std::function<Int64(Int64)> calc_watermark_ts;
    if (params.delay_interval != 0)
    {
        calc_watermark_ts = [this](Int64 event_ts) {
            auto event_ts_bias = addTime(
                event_ts,
                params.delay_interval_kind,
                -1 * params.delay_interval,
                *params.window_params->time_zone,
                params.window_params->time_scale);

            if constexpr (apply_watermark_per_row)
                return event_ts_bias;
            else
                return calculateWatermark(event_ts_bias);
        };
    }
    else
    {
        if constexpr (apply_watermark_per_row)
            calc_watermark_ts = [](Int64 event_ts) { return event_ts; };
        else
            calc_watermark_ts = [this](Int64 event_ts) { return calculateWatermark(event_ts); };
    }

    Int64 event_ts_watermark = watermark_ts;

    /// [Process chunks]
    /// 1) filter and collect late events by @param watermark_ts
    /// 2) update max event timestamp
    auto columns = chunk.detachColumns();
    const auto & time_vec = assert_cast<const TimeColumnType &>(*columns[time_col_pos]).getData();

    /// FIXME, use simple FilterTransform to do this ?
    auto rows = time_vec.size();
    IColumn::Filter filt(rows, 1);

    UInt64 late_events_in_chunk = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        const auto & event_ts = time_vec[i];
        if (event_ts > max_event_ts)
        {
            max_event_ts = event_ts;

            if constexpr (apply_watermark_per_row)
                event_ts_watermark = calc_watermark_ts(max_event_ts);
        }

        if (unlikely(event_ts < event_ts_watermark))
        {
            filt[i] = 0;
            ++late_events_in_chunk;
        }
    }

    if constexpr (!apply_watermark_per_row)
        event_ts_watermark = calc_watermark_ts(max_event_ts);

    if (late_events_in_chunk > 0)
    {
        late_events += late_events_in_chunk;
        for (auto & column : columns)
            column = column->filter(filt, rows - late_events_in_chunk);
    }

    chunk.setColumns(columns, columns[0]->size());

    /// [Update new watermark]
    /// Use max event time as new watermark
    if (event_ts_watermark > watermark_ts)
    {
        auto chunk_ctx = chunk.getOrCreateChunkContext();
        chunk_ctx->setWatermark(event_ts_watermark);
        watermark_ts = event_ts_watermark;
    }
}

Int64 WatermarkStamper::calculateWatermark(Int64 event_ts) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "calculateWatermark() not implemented in {}", getName());
}

VersionType WatermarkStamper::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType WatermarkStamper::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}

void WatermarkStamper::serialize(WriteBuffer & wb) const
{
    /// WatermarkStamper has its own version than WatermarkTransform
    writeIntBinary(getVersion(), wb);

    writeIntBinary(max_event_ts, wb);
    writeIntBinary(watermark_ts, wb);
    writeIntBinary(late_events, wb);
    writeIntBinary(last_logged_late_events, wb);
    writeIntBinary(last_logged_late_events_ts, wb);
}

void WatermarkStamper::deserialize(ReadBuffer & rb)
{
    version = 0;

    readIntBinary(*version, rb);
    readIntBinary(max_event_ts, rb);
    readIntBinary(watermark_ts, rb);
    readIntBinary(late_events, rb);
    readIntBinary(last_logged_late_events, rb);
    readIntBinary(last_logged_late_events_ts, rb);
}
}
}
