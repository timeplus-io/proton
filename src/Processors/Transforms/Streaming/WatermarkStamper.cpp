#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Interpreters/Streaming/TimeTransformHelper.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
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
        if (emit->delay_interval)
            throw Exception("Streaming doesn't support having both delay and periodic emit", ErrorCodes::INCORRECT_QUERY);

        if (emit->timeout_interval)
            throw Exception("Streaming doesn't support having both timeout and periodic emit", ErrorCodes::INCORRECT_QUERY);

        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());

        if (params.window_params)
            params.mode = emit->on_update ? EmitMode::PeriodicWatermarkOnUpdate : EmitMode::PeriodicWatermark;
        else
            params.mode = emit->on_update ? EmitMode::PeriodicOnUpdate : EmitMode::Periodic;
    }
    else if (emit->after_watermark)
    {
        if (!params.window_params)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "Watermark emit is only supported in streaming queries with streaming window function");

        params.mode = EmitMode::Watermark;
    }
    else if (emit->on_update)
    {
        if (params.window_params)
            params.mode = EmitMode::WatermarkOnUpdate;
        else
            params.mode = EmitMode::OnUpdate;
    }
    else
        params.mode = EmitMode::None;

    if (emit->timeout_interval)
        params.timeout_interval = extractInterval(emit->timeout_interval->as<ASTFunction>());

    if (emit->delay_interval)
        params.delay_interval = extractInterval(emit->delay_interval->as<ASTFunction>());
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
        if (mode != EmitMode::Tail && mode != EmitMode::None)
            throw Exception("Streaming tail mode doesn't support any watermark or periodic emit", ErrorCodes::INCORRECT_QUERY);

        /// Set default emit mode
        if (mode == EmitMode::None)
            mode = EmitMode::Tail;
    }
    else
    {
        /// For streaming aggregation query
        if (mode == EmitMode::Tail)
            throw Exception("Streaming aggregation doesn't support tail emit", ErrorCodes::INCORRECT_QUERY);

        /// Set default emit mode
        if (mode == EmitMode::None)
        {
            if (window_params)
            {
                mode = EmitMode::Watermark;
            }
            else
            {
                /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
                mode = EmitMode::Periodic;
                periodic_interval.interval = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first;
                periodic_interval.unit = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second;
            }
        }
    }
}

void WatermarkStamper::preProcess(const Block & header)
{
    switch (params.mode)
    {
        case EmitMode::Periodic:
        case EmitMode::PeriodicOnUpdate:
        {
            initPeriodicTimer(params.periodic_interval);
            break;
        }
        case EmitMode::PeriodicWatermark:
        case EmitMode::PeriodicWatermarkOnUpdate:
        {
            initPeriodicTimer(params.periodic_interval);
            [[fallthrough]];
        }
        case EmitMode::Watermark:
        case EmitMode::WatermarkOnUpdate:
        {
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
    if (params.timeout_interval)
        initTimeoutTimer(params.timeout_interval);
}

ALWAYS_INLINE Int64 WatermarkStamper::calculateWatermark(Int64 event_ts) const
{
    if (params.delay_interval)
    {
        auto event_ts_bias = addTime(
            event_ts,
            params.delay_interval.unit,
            -1 * params.delay_interval.interval,
            *params.window_params->time_zone,
            params.window_params->time_scale);

        return calculateWatermarkImpl(event_ts_bias);
    }
    else
        return calculateWatermarkImpl(event_ts);
}

ALWAYS_INLINE Int64 WatermarkStamper::calculateWatermarkPerRow(Int64 event_ts) const
{
    if (params.delay_interval)
       return addTime(
            event_ts,
            params.delay_interval.unit,
            -1 * params.delay_interval.interval,
            *params.window_params->time_zone,
            params.window_params->time_scale);
    else
        return event_ts;
}

void WatermarkStamper::processAfterUnmuted(Chunk & chunk)
{
    assert(!chunk.hasRows());

    switch (params.mode)
    {
        case EmitMode::Periodic:
        case EmitMode::PeriodicOnUpdate:
        {
            processPeriodic(chunk, /*use_processing_time*/true);
            break;
        }
        case EmitMode::OnUpdate:
        {
            chunk.setWatermark(MonotonicNanoseconds::now());
            break;
        }
        case EmitMode::Watermark:
        {
            auto muted_watermark_ts = params.window_params->type == WindowType::Session ? calculateWatermarkPerRow(max_event_ts)
                                                                                        : calculateWatermark(max_event_ts);
            if (muted_watermark_ts != INVALID_WATERMARK) [[likely]]
            {
                watermark_ts = muted_watermark_ts;
                chunk.setWatermark(watermark_ts);
            }
            break;
        }
        case EmitMode::PeriodicWatermark:
        case EmitMode::PeriodicWatermarkOnUpdate:
        {
            auto muted_watermark_ts = params.window_params->type == WindowType::Session ? calculateWatermarkPerRow(max_event_ts)
                                                                                        : calculateWatermark(max_event_ts);
            if (muted_watermark_ts != INVALID_WATERMARK) [[likely]]
                watermark_ts = muted_watermark_ts;

            processPeriodic(chunk, /*use_processing_time*/ false);
            break;
        }
        default:
            break;
    }
}

void WatermarkStamper::processWithMutedWatermark(Chunk & chunk)
{
    /// NOTE: In order to avoid that when there is only backfill data and no new data, the window aggregation don't emit results after the backfill is completed.
    /// Even mute watermark, we still need collect `max_event_ts` which will be used in "processAfterUnmuted()" to emit a watermark as soon as the backfill is completed
    if (chunk.hasRows() && time_col_pos >= 0 && params.window_params)
    {
        if (params.window_params->time_col_is_datetime64)
            max_event_ts = std::max<Int64>(
                max_event_ts,
                *std::ranges::max_element(assert_cast<const ColumnDateTime64 &>(*chunk.getColumns()[time_col_pos]).getData()));
        else
            max_event_ts = std::max<Int64>(
                max_event_ts,
                *std::ranges::max_element(assert_cast<const ColumnDateTime &>(*chunk.getColumns()[time_col_pos]).getData()));
    }

    processTimeout(chunk);
    logLateEvents();
}

void WatermarkStamper::process(Chunk & chunk)
{
    switch (params.mode)
    {
        case EmitMode::PeriodicOnUpdate:
            [[fallthrough]]; /// Emit only keyed and changed states for aggregating
        case EmitMode::Periodic:
        {
            processPeriodic(chunk, /*use_processing_time=*/true);
            break;
        }
        case EmitMode::OnUpdate:
        {
            if (chunk.hasRows())
                chunk.setWatermark(MonotonicNanoseconds::now());
            break;
        }
        case EmitMode::Watermark:
        {
            processWatermark(chunk);
            break;
        }
        case EmitMode::PeriodicWatermarkOnUpdate:
            [[fallthrough]]; /// Emit only keyed and changed states for aggregating
        case EmitMode::PeriodicWatermark:
        {
            processWatermark(chunk);
            /// Clear watermark and set it in `processPeriodic()`
            chunk.clearWatermark();
            processPeriodic(chunk, /*use_processing_time=*/false);
            break;
        }
        case EmitMode::WatermarkOnUpdate:
        {
            processWatermark(chunk);
            /// Always emit the watermark for each batch of events
            if (chunk.hasRows())
                chunk.setWatermark(watermark_ts);
            break;
        }
        default:
            break;
    }

    processTimeout(chunk);
    logLateEvents();
}

void WatermarkStamper::processPeriodic(Chunk & chunk, bool use_processing_time)
{
    assert(next_periodic_emit_ts);

    /// FIXME: use a Timer.
    auto now = MonotonicNanoseconds::now();
    if (now < next_periodic_emit_ts)
        return;

    next_periodic_emit_ts = now + periodic_interval;

    chunk.setWatermark(use_processing_time ? now : watermark_ts);
}

void WatermarkStamper::processTimeout(Chunk & chunk)
{
    if (next_timeout_emit_ts == 0)
        return;

    /// FIXME: use a Timer.
    auto now = MonotonicNanoseconds::now();

    /// Update next timeout ts if emitted
    if (chunk.hasRows())
    {
        next_timeout_emit_ts = now + timeout_interval;
        return;
    }

    if (now < next_timeout_emit_ts)
        return;

    watermark_ts = max_event_ts + 1;
    next_timeout_emit_ts = now + timeout_interval;

    chunk.setWatermark(TIMEOUT_WATERMARK);
    LOG_DEBUG(log, "Timeout emit time={}, rows={}", now, chunk.getNumRows());
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
void WatermarkStamper::processWatermarkImpl(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    assert(params.window_params);

    Int64 event_ts_watermark = watermark_ts;

    /// [Process chunks]
    /// 1) filter and collect late events by @param watermark_ts
    /// 2) update max event timestamp
    auto columns = chunk.detachColumns();
    const auto & time_vec = assert_cast<const TimeColumnType &>(*columns[time_col_pos]).getData();

    /// FIXME, use simple FilterTransform to do this ?
    auto rows = time_vec.size();
    IColumn::Filter filter(rows, 1);

    UInt64 late_events_in_chunk = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        const auto & event_ts = time_vec[i];
        if (event_ts > max_event_ts)
        {
            max_event_ts = event_ts;

            if constexpr (apply_watermark_per_row)
                event_ts_watermark = calculateWatermarkPerRow(max_event_ts);
        }

        if (unlikely(event_ts < event_ts_watermark))
        {
            filter[i] = 0;
            ++late_events_in_chunk;
        }
    }

    if constexpr (!apply_watermark_per_row)
        event_ts_watermark = calculateWatermark(max_event_ts);

    if (late_events_in_chunk > 0)
    {
        late_events += late_events_in_chunk;
        for (auto & column : columns)
            column = column->filter(filter, rows - late_events_in_chunk);
    }

    chunk.setColumns(columns, columns[0]->size());

    /// [Update new watermark]
    /// Use max event time as new watermark
    if (event_ts_watermark > watermark_ts)
    {
        chunk.setWatermark(event_ts_watermark);
        watermark_ts = event_ts_watermark;
    }
}

void WatermarkStamper::processWatermark(Chunk & chunk)
{
    assert(params.window_params);
    if (params.window_params->type == WindowType::Session)
    {
        if (params.window_params->time_col_is_datetime64)
            processWatermarkImpl<ColumnDateTime64, true>(chunk);
        else
            processWatermarkImpl<ColumnDateTime, true>(chunk);
    }
    else
    {
        if (params.window_params->time_col_is_datetime64)
            processWatermarkImpl<ColumnDateTime64, false>(chunk);
        else
            processWatermarkImpl<ColumnDateTime, false>(chunk);
    }
}

Int64 WatermarkStamper::calculateWatermarkImpl(Int64 event_ts) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "calculateWatermarkImpl() not implemented in {}", getName());
}

void WatermarkStamper::initPeriodicTimer(const WindowInterval & interval)
{
    if (interval.unit > IntervalKind::Day)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The maximum interval kind of streaming periodic emit policy is day, but got {}",
            magic_enum::enum_name(interval.unit));

    periodic_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Nanosecond);
    next_periodic_emit_ts = MonotonicNanoseconds::now() + periodic_interval;
}

void WatermarkStamper::initTimeoutTimer(const WindowInterval & interval)
{
    if (interval.unit > IntervalKind::Day)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The maximum interval kind of emit timeout is day, but got {}",
            magic_enum::enum_name(interval.unit));

    timeout_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Nanosecond);
    next_timeout_emit_ts = MonotonicNanoseconds::now() + timeout_interval;
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
