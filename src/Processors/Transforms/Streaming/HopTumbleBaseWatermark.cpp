#include "HopTumbleBaseWatermark.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Streaming/FunctionsStreamingWindow.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Chunk.h>
#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/DateLUT.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace Streaming
{
namespace
{
using ColumnDateTime64 = ColumnDecimal<DateTime64>;
using ColumnDateTime32 = ColumnVector<UInt32>;

template <typename TargetColumnType>
ALWAYS_INLINE void
doProcessChunk(Chunk & chunk, UInt16 time_col_position, Int64 last_project_watermark_ts, Int64 & max_event_ts, UInt64 & late_events)
{
    auto columns = chunk.detachColumns();

    /// FIXME, use simple FilterTransform to do this ?
    const typename TargetColumnType::Container & time_vec
        = checkAndGetColumn<TargetColumnType>(columns[time_col_position].get())->getData();
    auto rows = time_vec.size();
    IColumn::Filter filt(rows, 1);

    UInt64 late_events_in_chunk = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        if (time_vec[i] > max_event_ts)
            max_event_ts = time_vec[i];

        if (time_vec[i] < last_project_watermark_ts)
        {
            filt[i] = 0;
            late_events_in_chunk += 1;
        }
    }

    if (late_events_in_chunk > 0)
    {
        late_events += late_events_in_chunk;
        for (auto & column : columns)
            column = column->filter(filt, rows - late_events_in_chunk);
    }

    chunk.setColumns(columns, columns[0]->size());
}
}

HopTumbleBaseWatermark::HopTumbleBaseWatermark(
    WatermarkSettings && watermark_settings_, size_t time_col_position_, bool proc_time_, Poco::Logger * log_)
    : Watermark(std::move(watermark_settings_), proc_time_, log_), time_col_position(time_col_position_)
{
    assert(watermark_settings.window_desc);
    assert(!watermark_settings.window_desc->argument_names.empty());

    if (watermark_settings.mode == WatermarkSettings::EmitMode::NONE)
        watermark_settings.mode = WatermarkSettings::EmitMode::WATERMARK;

    const auto & desc = *watermark_settings.window_desc;
    time_col_is_datetime64 = isDateTime64(desc.argument_types[0]);

    if (time_col_is_datetime64)
        scale = checkAndGetDataType<DataTypeDateTime64>(desc.argument_types[0].get())->getScale();

    multiplier = intExp10(std::abs(scale - 3));
}

void HopTumbleBaseWatermark::init(Int64 & interval)
{
    auto * func_ast = watermark_settings.window_desc->func_ast->as<ASTFunction>();
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

void HopTumbleBaseWatermark::doProcess(Chunk & chunk)
{
    /// We don't support column type changes in the middle of a streaming query
    if (time_col_is_datetime64)
    {
        /// const auto & datetime64_col = static_cast<const ColumnDateTime64 &>(*time_col.column);
        /// const ColumnDateTime64::Container & time_vec = datetime64_col.getData();
        /// auto scale = datetime64_col.getScale();
        doProcessChunk<ColumnDateTime64>(chunk, time_col_position, last_projected_watermark_ts, max_event_ts, late_events);
    }
    else
    {
        doProcessChunk<ColumnDateTime32>(chunk, time_col_position, last_projected_watermark_ts, max_event_ts, late_events);
    }

    if (late_events > last_logged_late_events)
    {
        if (MonotonicSeconds::now() - last_logged_late_events_ts >= 5)
        {
            LOG_INFO(log, "Found {} late events for data. Last projected watermark={}", late_events, last_projected_watermark_ts);
            last_logged_late_events_ts = MonotonicSeconds::now();
            last_logged_late_events = late_events;
        }
    }

    if (chunk.hasRows())
    {
        has_event_in_window = true;
        last_event_seen_ts = UTCSeconds::now();
        assignWatermark(chunk);
    }
}

void HopTumbleBaseWatermark::processWatermark(Chunk & chunk)
{
    if (scale != 0)
        processWatermarkWithAutoScale(chunk);
    else
        doProcessWatermark(chunk);
}

ALWAYS_INLINE void HopTumbleBaseWatermark::processWatermarkWithAutoScale(Chunk & chunk)
{
    assert(scale != 0);

    if (likely(watermark_ts != 0))
    {
        auto interval = getProgressingInterval();

        /// FIXME, use multiply for optimization instead of loop ?
        /// Multiply only has advantage when max_event_ts is way bigger than current watermark_ts
        /// which causes quite a few loops. But this is time skew and abnormal
        Int64 final_watermark = 0;
        while (watermark_ts <= max_event_ts)
        {
            /// emit the max watermark
            last_projected_watermark_ts = watermark_ts;
            final_watermark = watermark_ts;
            watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
        }

        if (final_watermark > 0)
        {
            auto watermark_lower_bound = addTimeWithAutoScale(final_watermark, window_interval_kind, -1 * window_interval);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(final_watermark, watermark_lower_bound);
            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", final_watermark, watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow(chunk);
}

ALWAYS_INLINE void HopTumbleBaseWatermark::doProcessWatermark(Chunk & chunk)
{
    assert(scale == 0);

    if (likely(watermark_ts != 0))
    {
        auto interval = getProgressingInterval();
        /// FIXME, use multiply for optimization instead of loop
        Int64 final_watermark = 0;
        while (watermark_ts <= max_event_ts)
        {
            /// emit the max watermark
            last_projected_watermark_ts = watermark_ts;
            final_watermark = watermark_ts;
            watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
        }

        if (final_watermark > 0)
        {
            auto watermark_lower_bound = addTime(final_watermark, window_interval_kind, -1 * window_interval, *timezone);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(final_watermark, watermark_lower_bound);
            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", final_watermark, watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow(chunk);
}

void HopTumbleBaseWatermark::processWatermarkWithDelay(Chunk & chunk)
{
    if (scale != 0)
        processWatermarkWithDelayAndWithAutoScale(chunk);
    else
        doProcessWatermarkWithDelay(chunk);
}

ALWAYS_INLINE void HopTumbleBaseWatermark::processWatermarkWithDelayAndWithAutoScale(Chunk & chunk)
{
    assert(scale != 0);
    if (likely(watermark_ts != 0))
    {
        Int64 watermark_ts_bias
            = addTimeWithAutoScale(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval);

        auto interval = getProgressingInterval();
        Int64 final_watermark = 0;
        while (watermark_ts_bias <= max_event_ts)
        {
            last_projected_watermark_ts = watermark_ts;
            final_watermark = watermark_ts;

            watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            watermark_ts_bias
                = addTimeWithAutoScale(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval);
        }

        /// If we never projected
        if (unlikely(last_projected_watermark_ts == 0))
        {
            auto watermark_ts_lower_bound = addTimeWithAutoScale(watermark_ts, window_interval_kind, -1 * interval);
            watermark_ts_bias = addTimeWithAutoScale(
                watermark_ts_lower_bound, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval);
            if (max_event_ts >= watermark_ts_bias)
            {
                final_watermark = watermark_ts_lower_bound;
                last_projected_watermark_ts = watermark_ts_lower_bound;
            }
        }

        if (final_watermark > 0)
        {
            auto watermark_lower_bound = addTimeWithAutoScale(final_watermark, window_interval_kind, -1 * window_interval);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(final_watermark, watermark_lower_bound);
            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", final_watermark, watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow(chunk, true);
}

ALWAYS_INLINE void HopTumbleBaseWatermark::doProcessWatermarkWithDelay(Chunk & chunk)
{
    assert(scale == 0);
    if (likely(watermark_ts != 0))
    {
        Int64 watermark_ts_bias
            = addTime(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);

        auto interval = getProgressingInterval();
        Int64 final_watermark = 0;
        while (watermark_ts_bias <= max_event_ts)
        {
            last_projected_watermark_ts = watermark_ts;
            final_watermark = watermark_ts;

            watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
            watermark_ts_bias
                = addTime(watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);
        }

        /// If we never projected
        if (unlikely(last_projected_watermark_ts == 0))
        {
            auto watermark_ts_lower_bound = addTime(watermark_ts, window_interval_kind, -1 * interval, *timezone);
            watermark_ts_bias = addTime(
                watermark_ts_lower_bound, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);
            if (max_event_ts >= watermark_ts_bias)
            {
                final_watermark = watermark_ts_lower_bound;
                last_projected_watermark_ts = watermark_ts_lower_bound;
            }
        }

        if (final_watermark > 0)
        {
            auto watermark_lower_bound = addTime(final_watermark, window_interval_kind, -1 * window_interval, *timezone);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(final_watermark, watermark_lower_bound);
            LOG_INFO(log, "Emitted watermark={}, watermark_lower_bound={}", final_watermark, watermark_lower_bound);
        }
    }
    else
        std::tie(last_projected_watermark_ts, watermark_ts) = initFirstWindow(chunk, true);
}

void HopTumbleBaseWatermark::handleIdlenessWatermark(Chunk & chunk)
{
    if (proc_time && watermark_ts > 0)
    {
        auto interval = getProgressingInterval();
        if (scale != 0)
        {
            auto now = UTCMilliseconds::now();
            if (scale > 3)
                now *= multiplier;
            else if (scale < 3)
                now /= multiplier;

            if (now >= watermark_ts)
            {
                auto watermark_lower_bound = addTimeWithAutoScale(watermark_ts, window_interval_kind, -1 * window_interval);
                auto chunk_ctx = chunk.getOrCreateChunkContext();
                chunk_ctx->setWatermark(watermark_ts, watermark_lower_bound);

                last_projected_watermark_ts = watermark_ts;
                watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            }
        }
        else
        {
            if (UTCSeconds::now() >= watermark_ts)
            {
                auto watermark_lower_bound = addTime(watermark_ts, window_interval_kind, -1 * window_interval, *timezone);
                auto chunk_ctx = chunk.getOrCreateChunkContext();
                chunk_ctx->setWatermark(watermark_ts, watermark_lower_bound);

                last_projected_watermark_ts = watermark_ts;
                watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
            }
        }
    }

    if (has_event_in_window && watermark_settings.emit_timeout_interval != 0 && last_event_seen_ts != 0)
    {
        auto now = UTCSeconds::now();
        if (addTime(now, watermark_settings.emit_timeout_interval_kind, -1 * watermark_settings.emit_timeout_interval, *timezone)
            > last_event_seen_ts)
        {
            has_event_in_window = false;
            last_event_seen_ts = now;

            auto interval = getProgressingInterval();

            if (scale != 0)
            {
                auto watermark_lower_bound = addTimeWithAutoScale(watermark_ts, window_interval_kind, -1 * window_interval);
                auto chunk_ctx = chunk.getOrCreateChunkContext();
                chunk_ctx->setWatermark(watermark_ts, watermark_lower_bound);

                last_projected_watermark_ts = watermark_ts;
                watermark_ts = addTimeWithAutoScale(watermark_ts, window_interval_kind, interval);
            }
            else
            {
                auto watermark_lower_bound = addTime(watermark_ts, window_interval_kind, -1 * window_interval, *timezone);
                auto chunk_ctx = chunk.getOrCreateChunkContext();
                chunk_ctx->setWatermark(watermark_ts, watermark_lower_bound);

                last_projected_watermark_ts = watermark_ts;
                watermark_ts = addTime(watermark_ts, window_interval_kind, interval, *timezone);
            }
        }
    }
}

void HopTumbleBaseWatermark::handleIdlenessWatermarkWithDelay(Chunk & chunk)
{
    handleIdlenessWatermark(chunk);
}

std::pair<Int64, Int64> HopTumbleBaseWatermark::initFirstWindow(Chunk & chunk, bool delay) const
{
    if (max_event_ts > 0)
    {
        if (scale != 0)
            return initFirstWindowWithAutoScale(chunk, delay);
        else
            return doInitFirstWindow(chunk, delay);
    }

    return {watermark_ts, watermark_ts};
}

std::pair<Int64, Int64> HopTumbleBaseWatermark::initFirstWindowWithAutoScale(Chunk & chunk, bool delay) const
{
    assert(time_col_is_datetime64);
    assert(scale != 0);

    const auto & time_col = chunk.getColumns()[time_col_position];
    const auto & time_vec = checkAndGetColumn<ColumnDateTime64>(time_col.get())->getData();
    auto min_event_ts = std::min_element(time_vec.begin(), time_vec.end());

    /// Scale in and scale out, if it is a DateTime64 as addTime doesn't supports scale yet. FIXME
    auto scaled_watermark_ts_secs = DecimalUtils::getWholePart(DateTime64(max_event_ts), scale);
    auto window = getWindow(scaled_watermark_ts_secs);

    std::pair<Int64, Int64> result
        = {DecimalUtils::decimalFromComponents<DateTime64>(window.first, 0, scale),
           DecimalUtils::decimalFromComponents<DateTime64>(window.second, 0, scale)};

    if (result.first > *min_event_ts)
    {
        bool emit_watermark = true;

        if (delay)
        {
            /// if delay, max_event_ts >= watermark_lower_bound + delay
            auto watermark_ts_bias
                = addTime(window.first, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);
            watermark_ts_bias = DecimalUtils::decimalFromComponents<DateTime64>(watermark_ts_bias, 0, scale);
            if (max_event_ts < watermark_ts_bias)
                emit_watermark = false;
        }

        if (emit_watermark)
        {
            /// The first chunk contains at least a full window. Emit the watermark
            auto watermark_lower_bound_prev = addTime(window.first, window_interval_kind, -1 * getProgressingInterval(), *timezone);
            auto watermark_lower_bound = DecimalUtils::decimalFromComponents<DateTime64>(watermark_lower_bound_prev, 0, scale);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(result.first, watermark_lower_bound);
            return result;
        }
    }

    return {0, result.second};
}

std::pair<Int64, Int64> HopTumbleBaseWatermark::doInitFirstWindow(Chunk & chunk, bool delay) const
{
    assert(scale == 0);

    Int64 min_event_ts = 0;
    const auto & time_col = chunk.getColumns()[time_col_position];
    if (time_col_is_datetime64)
    {
        const auto & time_vec = checkAndGetColumn<ColumnDateTime64>(time_col.get())->getData();
        min_event_ts = *std::min_element(time_vec.begin(), time_vec.end());
    }
    else
    {
        const auto & time_vec = checkAndGetColumn<ColumnDateTime32>(time_col.get())->getData();
        min_event_ts = *std::min_element(time_vec.begin(), time_vec.end());
    }

    auto result = getWindow(max_event_ts);

    if (result.first > min_event_ts)
    {
        bool emit_watermark = true;
        if (delay)
        {
            /// if delay, max_event_ts >= watermark_lower_bound + delay
            auto watermark_ts_bias
                = addTime(result.first, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, *timezone);
            if (max_event_ts < watermark_ts_bias)
                emit_watermark = false;
        }

        if (emit_watermark)
        {
            auto watermark_lower_bound = addTime(result.first, window_interval_kind, -1 * getProgressingInterval(), *timezone);
            auto chunk_ctx = chunk.getOrCreateChunkContext();
            chunk_ctx->setWatermark(result.first, watermark_lower_bound);
            return result;
        }
    }

    return {0, result.second};
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

void HopTumbleBaseWatermark::serialize(WriteBuffer & wb) const
{
    writeIntBinary(has_event_in_window, wb);

    Watermark::serialize(wb);
}

void HopTumbleBaseWatermark::deserialize(ReadBuffer & rb)
{
    readIntBinary(has_event_in_window, rb);

    Watermark::deserialize(rb);
}
}
}
