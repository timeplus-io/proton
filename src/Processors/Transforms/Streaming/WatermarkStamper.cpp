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
#include <Processors/Transforms/Streaming/HopHelper.h>
#include <Processors/Transforms/Streaming/SessionHelper.h>
#include <Processors/Transforms/Streaming/TumbleHelper.h>
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
extern const int UNSUPPORTED_WATERMARK_STRATEGY;
}

namespace Streaming
{
namespace
{
void mergeEmitQuerySettings(const ASTPtr & emit_query, WatermarkStamperParams & params)
{
    if (!emit_query)
        return;

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    params.strategy = emit->watermark_strategy;
    if (params.strategy == WatermarkStrategy::BoundedOutOfOrderness)
    {
        if (!emit->delay_interval) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "Missing 'DELAY {}' in emit clause after watermark strategy 'BoundedOutOfOrderness'");

        params.delay_interval = extractInterval(emit->delay_interval->as<ASTFunction>());
    }

    params.mode = WatermarkEmitMode::Unknown;
    if (emit->periodic_interval)
    {
        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());
        params.mode = WatermarkEmitMode::Periodic;
    }

    if (emit->on_update)
        params.mode = params.mode == WatermarkEmitMode::Periodic ? WatermarkEmitMode::PeriodicOnUpdate : WatermarkEmitMode::OnUpdate;

    if (emit->timeout_interval)
    {
        if (params.mode == WatermarkEmitMode::Periodic)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "The 'TIMEOUT {}' in emit clause conflicts with watermark emit mode '{}'",
                emit->timeout_interval->formatForErrorMessage(),
                magic_enum::enum_name(params.mode));

        params.timeout_interval = extractInterval(emit->timeout_interval->as<ASTFunction>());
    }
}

void initWatermarkStrategyAndEmitModeForWindowAggr(WatermarkStrategy & strategy, WatermarkEmitMode & mode, WindowParams & params)
{
    switch (params.type)
    {
        case WindowType::TUMBLE:
            return TumbleHelper::validateWatermarkStrategyAndEmitMode(strategy, mode, assert_cast<TumbleWindowParams &>(params));
        case WindowType::HOP:
            return HopHelper::validateWatermarkStrategyAndEmitMode(strategy, mode, assert_cast<HopWindowParams &>(params));
        case WindowType::SESSION:
            return SessionHelper::validateWatermarkStrategyAndEmitMode(strategy, mode, assert_cast<SessionWindowParams &>(params));
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No support window type: {}", magic_enum::enum_name(params.type));
    }
    UNREACHABLE();
}

Int64 calculateWatermarkBasedOnWindow(Int64 event_ts, WindowParams & params)
{
    switch (params.type)
    {
        case WindowType::TUMBLE:
            return TumbleHelper::getLastFinalizedWindow(event_ts, assert_cast<TumbleWindowParams &>(params)).end;
        case WindowType::HOP:
            return HopHelper::getLastFinalizedWindow(event_ts, assert_cast<HopWindowParams &>(params)).end;
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "No support calculate wateramrk based on {} window", magic_enum::enum_name(params.type));
    }
    UNREACHABLE();
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
        if (strategy != WatermarkStrategy::Unknown || mode != WatermarkEmitMode::Unknown)
            throw Exception(
                ErrorCodes::UNSUPPORTED_WATERMARK_STRATEGY, "Streaming tail mode doesn't support any watermark strategy and emit mode");

        strategy = WatermarkStrategy::None;
        mode = WatermarkEmitMode::None;
    }
    else
    {
        /// For streaming aggregation query
        if (window_params)
            initWatermarkStrategyAndEmitModeForWindowAggr(strategy, mode, *window_params);
        else
        {
            /// If no emit mode is given for global aggregates, use default peridoci emit
            if (strategy == WatermarkStrategy::Unknown)
                strategy = WatermarkStrategy::None;
            else if (strategy != WatermarkStrategy::None)
                throw Exception(
                    ErrorCodes::UNSUPPORTED_WATERMARK_STRATEGY,
                    "Streaming global aggregates doesn't support any watermark strategy, but got '{}'",
                    magic_enum::enum_name(strategy));

            if (mode == WatermarkEmitMode::Unknown)
            {
                mode = WatermarkEmitMode::Periodic;
                periodic_interval.interval = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first;
                periodic_interval.unit = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second;
            }
            else if (
                mode != WatermarkEmitMode::Periodic && mode != WatermarkEmitMode::PeriodicOnUpdate && mode != WatermarkEmitMode::OnUpdate)
                throw Exception(
                    ErrorCodes::UNSUPPORTED_WATERMARK_STRATEGY,
                    "Streaming global aggregates doesn't support the emit mode '{}'",
                    magic_enum::enum_name(mode));
        }
    }
}

String WatermarkStamper::getDescription() const
{
    return fmt::format("{}-{}", magic_enum::enum_name(params.strategy), magic_enum::enum_name(params.mode));
}

void WatermarkStamper::preProcess(const Block & header)
{
    assert(params.strategy != WatermarkStrategy::Unknown);
    assert(params.mode != WatermarkEmitMode::Unknown);

    if (params.strategy != WatermarkStrategy::None)
    {
        assert(params.window_params);
        if (!header.has(params.window_params->desc->argument_names[0]))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The event time columns not found");

        time_col_pos = header.getPositionByName(params.window_params->desc->argument_names[0]);
    }

    if (params.periodic_interval)
    {
        if (params.periodic_interval.unit > IntervalKind::Day)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The maximum interval kind of emit periodic is day, but got {}",
                magic_enum::enum_name(params.periodic_interval.unit));

        auto periodic_interval = BaseScaleInterval::toBaseScale(params.periodic_interval).toIntervalKind(IntervalKind::Nanosecond);
        periodic_timer.setInterval(periodic_interval);
        periodic_timer.reset();
    }

    if (params.timeout_interval)
    {
        if (params.timeout_interval.unit > IntervalKind::Day)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The maximum interval kind of emit timeout is day, but got {}",
                magic_enum::enum_name(params.timeout_interval.unit));

        auto timeout_interval = BaseScaleInterval::toBaseScale(params.timeout_interval).toIntervalKind(IntervalKind::Nanosecond);
        timeout_timer.setInterval(timeout_interval);
        timeout_timer.reset();
    }
}

void WatermarkStamper::processAfterUnmuted(Chunk & chunk)
{
    auto old_watermark_ts = watermark_ts;
    assert(!chunk.hasRows());

    applyWatermark(max_event_ts);

    emitWatermark(chunk, old_watermark_ts, true);
}

void WatermarkStamper::processWithMutedWatermark(Chunk & chunk)
{
    /// NOTE: In order to avoid that when there is only backfill data and no new data, the window aggregation don't emit results after the backfill is completed.
    /// Even mute watermark, we still need collect `max_event_ts` which will be used in "processAfterUnmuted()" to emit a watermark as soon as the backfill is completed
    if (chunk.hasRows() && params.strategy != WatermarkStrategy::None)
    {
        assert(params.window_params);
        if (params.window_params->time_col_is_datetime64)
            max_event_ts = std::max<Int64>(
                max_event_ts,
                *std::ranges::max_element(assert_cast<const ColumnDateTime64 &>(*chunk.getColumns()[time_col_pos]).getData()));
        else
            max_event_ts = std::max<Int64>(
                max_event_ts,
                *std::ranges::max_element(assert_cast<const ColumnDateTime &>(*chunk.getColumns()[time_col_pos]).getData()));
    }

    /// Need reset timeout timer (no need to check whether with timeout here)
    timeout_timer.reset();

    logLateEvents();
}

void WatermarkStamper::process(Chunk & chunk)
{
    auto old_watermark_ts = watermark_ts;
    bool has_events = chunk.hasRows();

    processWatermark(chunk);

    emitWatermark(chunk, old_watermark_ts, has_events);

    logLateEvents();
}

void WatermarkStamper::processWatermark(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    /// Reset timeout when event is seen
    timeout_timer.reset();

    switch (params.strategy)
    {
        case WatermarkStrategy::None:
            return;
        case WatermarkStrategy::Ascending:
        {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                return processWatermarkImpl<ColumnDateTime64, WatermarkStrategy::Ascending>(chunk);
            else
                return processWatermarkImpl<ColumnDateTime, WatermarkStrategy::Ascending>(chunk);
        }
        case WatermarkStrategy::OutOfOrdernessInBatch:
        {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                return processWatermarkImpl<ColumnDateTime64, WatermarkStrategy::OutOfOrdernessInBatch>(chunk);
            else
                return processWatermarkImpl<ColumnDateTime, WatermarkStrategy::OutOfOrdernessInBatch>(chunk);
        }
        case WatermarkStrategy::OutOfOrdernessInWindowAndBatch:
        {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                return processWatermarkImpl<ColumnDateTime64, WatermarkStrategy::OutOfOrdernessInWindowAndBatch>(chunk);
            else
                return processWatermarkImpl<ColumnDateTime, WatermarkStrategy::OutOfOrdernessInWindowAndBatch>(chunk);
        }
        case WatermarkStrategy::BoundedOutOfOrderness:
        {
            assert(params.window_params);
            if (params.window_params->time_col_is_datetime64)
                return processWatermarkImpl<ColumnDateTime64, WatermarkStrategy::BoundedOutOfOrderness>(chunk);
            else
                return processWatermarkImpl<ColumnDateTime, WatermarkStrategy::BoundedOutOfOrderness>(chunk);
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported watermark strategy '{}'", magic_enum::enum_name(params.strategy));
    }

    UNREACHABLE();
}

bool WatermarkStamper::applyWatermark(Int64 event_ts)
{

    switch (params.strategy)
    {
        case WatermarkStrategy::None:
            return false;
        case WatermarkStrategy::Ascending:
            return applyWatermarkImpl<WatermarkStrategy::Ascending>(event_ts);
        case WatermarkStrategy::OutOfOrdernessInBatch:
            return applyWatermarkImpl<WatermarkStrategy::OutOfOrdernessInBatch>(event_ts);
        case WatermarkStrategy::OutOfOrdernessInWindowAndBatch:
            return applyWatermarkImpl<WatermarkStrategy::OutOfOrdernessInWindowAndBatch>(event_ts);
        case WatermarkStrategy::BoundedOutOfOrderness:
            return applyWatermarkImpl<WatermarkStrategy::BoundedOutOfOrderness>(event_ts);
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported watermark strategy '{}'", magic_enum::enum_name(params.strategy));
    }

    UNREACHABLE();
}

template <typename TimeColumnType, WatermarkStrategy strategy>
void WatermarkStamper::processWatermarkImpl(Chunk & chunk)
{
    assert(chunk.hasRows());
    assert(params.window_params);

    constexpr bool apply_watermark_per_row
        = (strategy == WatermarkStrategy::Ascending || strategy == WatermarkStrategy::BoundedOutOfOrderness);

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
                applyWatermarkImpl<strategy>(max_event_ts);
        }

        if (unlikely(event_ts < watermark_ts))
        {
            filter[i] = 0;
            ++late_events_in_chunk;
        }
    }

    if constexpr (!apply_watermark_per_row)
        applyWatermarkImpl<strategy>(max_event_ts);

    if (late_events_in_chunk > 0)
    {
        late_events += late_events_in_chunk;
        for (auto & column : columns)
            column = column->filter(filter, rows - late_events_in_chunk);
    }

    chunk.setColumns(columns, columns[0]->size());
}

template <WatermarkStrategy strategy>
bool WatermarkStamper::applyWatermarkImpl(Int64 event_ts)
{
    Int64 event_ts_watermark = INVALID_WATERMARK;
    if constexpr (strategy == WatermarkStrategy::BoundedOutOfOrderness)
    {
        assert(params.delay_interval);
        event_ts_watermark = addTime(
            event_ts,
            params.delay_interval.unit,
            -1 * params.delay_interval.interval,
            *params.window_params->time_zone,
            params.window_params->time_scale);
    }
    else if constexpr (strategy == WatermarkStrategy::OutOfOrdernessInWindowAndBatch)
        event_ts_watermark = calculateWatermarkBasedOnWindow(event_ts, *params.window_params);
    else
        event_ts_watermark = event_ts;

    if (event_ts_watermark > watermark_ts)
    {
        watermark_ts = event_ts_watermark;
        return true;
    }
    return false;
}

void WatermarkStamper::emitWatermark(Chunk & chunk, Int64 old_watermark_ts, bool has_events)
{
    assert(!chunk.hasWatermark());

    auto emit_on_periodic = [&](std::optional<Int64> customized_watermark) {
        /// Emit watermark if timer is expired and not exists watermark
        auto reset_ts = periodic_timer.resetIfExpired();
        if (!reset_ts || chunk.hasWatermark())
            return false;

        chunk.setWatermark(customized_watermark.has_value() ? *customized_watermark : reset_ts);
        return true;
    };

    auto emit_on_window = [&, this]() {
        if (watermark_ts != old_watermark_ts)
        {
            /// Only emit the watermark when received new window
            assert(params.window_params);
            auto old_window_watermark = calculateWatermarkBasedOnWindow(old_watermark_ts, *params.window_params);
            auto new_window_watermark = calculateWatermarkBasedOnWindow(watermark_ts, *params.window_params);
            if (new_window_watermark != old_window_watermark)
            {
                chunk.setWatermark(watermark_ts);
                return true;
            }
        }
        return false;
    };

    auto emit_on_update = [&]() {
        if (has_events)
        {
            chunk.setWatermark(watermark_ts);
            return true;
        }
        return false;
    };

    auto emit_on_timeout = [&]() {
        /// Emit watermark if timer is expired
        if (timeout_timer.enabled() && timeout_timer.resetIfExpired())
        {
            chunk.setWatermark(TIMEOUT_WATERMARK);
            return true;
        }
        return false;
    };

    switch (params.mode)
    {
        case WatermarkEmitMode::None:
            return;
        case WatermarkEmitMode::PeriodicOnUpdate:
            [[fallthrough]]; /// Emit periodic watermark. Next, we will only emit the aggregation results of groups with updates in aggregation processing
        case WatermarkEmitMode::Periodic:
        {
            /// Speically, for tumble/hop window aggr, also emit immediately when the window is closed
            /// FIXME: need it ?
            if (params.window_params)
                emit_on_periodic(watermark_ts) || (params.window_params->type != WindowType::SESSION && emit_on_window());
            else
                emit_on_periodic(std::nullopt);

            return;
        }
        case WatermarkEmitMode::OnWindow:
        {
            emit_on_window() || emit_on_timeout();
            return;
        }
        case WatermarkEmitMode::OnUpdate:
        {
            emit_on_update() || emit_on_timeout();
            return;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported watermark emit mode '{}'", magic_enum::enum_name(params.mode));
    }

    UNREACHABLE();
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
