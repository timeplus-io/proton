#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Interpreters/Streaming/TimeTransformHelper.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/Streaming/HopWindowHelper.h>
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

String WatermarkStamper::getName() const
{
    return params.mode == EmitMode::Periodic ? "PeriodicWatermark" : "Watermark";
}

void WatermarkStamper::preProcess(const Block & header)
{
    switch (params.mode)
    {
        case EmitMode::None:
        case EmitMode::Tail:
        case EmitMode::AfterSessionClose:
        {
            break;
        }
        default:
        {
            if (params.periodic_interval)
                initPeriodicTimer(params.periodic_interval);

            if (params.window_params)
            {
                if (!header.has(params.window_params->desc->argument_names[0]))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The event time columns not found");

                time_col_pos = header.getPositionByName(params.window_params->desc->argument_names[0]);
            }
            break;
        }
    }

    /// WITH EMIT TIMEOUT
    if (params.mode != EmitMode::AfterSessionClose && params.timeout_interval)
        initTimeoutTimer(params.timeout_interval);
}

ALWAYS_INLINE Int64 WatermarkStamper::calculateWatermark(Int64 event_ts) const
{
    chassert(event_ts != INVALID_WATERMARK);
    if (params.delay_interval)
        event_ts = addTime(
            event_ts,
            params.delay_interval.unit,
            -1 * params.delay_interval.interval,
            *params.window_params->time_zone,
            params.window_params->time_scale);

    /// For tumble/hop window, by default we allow time skew within the same window
    chassert(params.window_params);
    switch (params.window_params->type)
    {
        case WindowType::Tumble:
        {
            auto & window_params = params.window_params->as<TumbleWindowParams &>();
            return toStartTime(
                event_ts, window_params.interval_kind, window_params.window_interval, *window_params.time_zone, window_params.time_scale);
        }
        case WindowType::Hop:
        {
            auto & window_params = params.window_params->as<HopWindowParams &>();
            auto last_finalized_window = HopWindowHelper::getLastFinalizedWindow(event_ts, window_params);
            return last_finalized_window.isValid() ? last_finalized_window.end : MIN_WATERMARK;
        }
        default:
        {
            return event_ts;
        }
    }
}

ALWAYS_INLINE Int64 WatermarkStamper::calculateWatermarkPerRow(Int64 event_ts) const
{
    assert(event_ts != INVALID_WATERMARK);
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
    chassert(!chunk.hasRows());

    if (useEventTime() && max_event_ts != INVALID_WATERMARK)
    {
        auto muted_watermark_ts
            = params.window_params->type == WindowType::Session ? calculateWatermarkPerRow(max_event_ts) : calculateWatermark(max_event_ts);
        if (muted_watermark_ts != INVALID_WATERMARK) [[likely]]
        {
            watermark_ts = muted_watermark_ts;
            chunk.setWatermark(watermark_ts);
        }
    }

    stampIfOnUpdate(chunk);
    processPeriodic(chunk);
}

void WatermarkStamper::processWithMutedWatermark(Chunk & chunk)
{
    /// NOTE: In order to avoid that when there is only backfill data and no new data, the window aggregation don't emit results after the backfill is completed.
    /// Even mute watermark, we still need collect `max_event_ts` which will be used in "processAfterUnmuted()" to emit a watermark as soon as the backfill is completed
    if (useEventTime() && chunk.hasRows())
    {
        chassert(params.window_params);
        /// FIXME: handle ColumnSparse/ColumnConst
        auto time_col = chunk.getColumns()[time_col_pos]->convertToFullColumnIfSparse()->convertToFullColumnIfConst();
        if (params.window_params->time_col_is_datetime64)
            max_event_ts
                = std::max<Int64>(max_event_ts, *std::ranges::max_element(assert_cast<const ColumnDateTime64 &>(*time_col).getData()));
        else
            max_event_ts
                = std::max<Int64>(max_event_ts, *std::ranges::max_element(assert_cast<const ColumnDateTime &>(*time_col).getData()));
    }

    processTimeout(chunk);
    logLateEvents();
}

void WatermarkStamper::process(Chunk & chunk)
{
    processWatermark(chunk);
    processPeriodic(chunk);
    processTimeout(chunk);
    logLateEvents();
}

void WatermarkStamper::processPeriodic(Chunk & chunk)
{
    if (next_periodic_emit_ts == 0)
        return;

    /// FIXME: use a Timer.
    auto now = MonotonicNanoseconds::now();
    if (now < next_periodic_emit_ts)
    {
        /// If enabled periodic emit, need clear watermark after `processWatermark()`
        chunk.clearWatermark();
        return;
    }

    next_periodic_emit_ts = now + periodic_interval;

    chunk.setWatermark(useEventTime() ? watermark_ts : now);
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
    LOG_DEBUG(logger, "Timeout emit time={}, rows={}", now, chunk.getNumRows());
}

void WatermarkStamper::logLateEvents()
{
    if (late_events > last_logged_late_events)
    {
        if (auto now = MonotonicSeconds::now(); now - last_logged_late_events_ts >= LOG_LATE_EVENTS_INTERVAL_SECONDS)
        {
            LOG_WARNING(
                logger,
                "Found {} late events for data (total={}). Last projected watermark={}",
                late_events - last_logged_late_events,
                late_events,
                watermark_ts);
            last_logged_late_events_ts = now;
            last_logged_late_events = late_events;
        }
    }
}

template <typename TimeColumnType, bool apply_watermark_per_row>
void WatermarkStamper::processWatermarkImpl(Chunk & chunk)
{
    chassert(params.window_params);

    Int64 event_ts_watermark = watermark_ts;

    /// [Process chunks]
    /// 1) filter and collect late events by watermark_ts
    /// 2) update max event timestamp
    auto columns = chunk.detachColumns();
    /// FIXME: handle ColumnSparse/ColumnConst
    auto time_col = columns[time_col_pos]->convertToFullColumnIfSparse()->convertToFullColumnIfConst();
    const auto & time_vec = assert_cast<const TimeColumnType &>(*time_col).getData();

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

        if (event_ts < event_ts_watermark)
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

void WatermarkStamper::processWithConsecutiveData(Chunk & chunk)
{
    /// Other modes need process() to advance processPeriodic / processTimeout timers.
    if (params.mode == EmitMode::OnUpdate && !chunk.hasRows())
        stampIfOnUpdate(chunk);
    else
        process(chunk);
}

void WatermarkStamper::stampIfOnUpdate(Chunk & chunk)
{
    if (params.mode == EmitMode::OnUpdate && !chunk.hasWatermark())
        chunk.setWatermark(useEventTime() ? watermark_ts : MonotonicNanoseconds::now());
}

void WatermarkStamper::processWatermark(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    if (useEventTime())
    {
        chassert(params.window_params);
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

    stampIfOnUpdate(chunk);
}

void WatermarkStamper::initPeriodicTimer(const WindowInterval & interval)
{
    if (interval.unit > IntervalKind::Kind::Day)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The maximum interval kind of streaming periodic emit policy is day, but got {}",
            magic_enum::enum_name(interval.unit));

    periodic_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Kind::Nanosecond);
    next_periodic_emit_ts = MonotonicNanoseconds::now() + periodic_interval;
}

void WatermarkStamper::initTimeoutTimer(const WindowInterval & interval)
{
    if (interval.unit > IntervalKind::Kind::Day)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The maximum interval kind of emit timeout is day, but got {}",
            magic_enum::enum_name(interval.unit));

    timeout_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Kind::Nanosecond);
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
