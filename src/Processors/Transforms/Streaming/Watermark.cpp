#include "Watermark.h"

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
#include <Common/logger_useful.h>
#include <Common/VersionRevision.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
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

    if (emit->periodic_interval)
    {
        if (emit->after_watermark || emit->delay_interval)
            throw Exception("Streaming doesn't support having both any watermark and periodic emit policy", ErrorCodes::SYNTAX_ERROR);

        extractInterval(
            emit->periodic_interval->as<ASTFunction>(),
            watermark_settings.emit_query_interval,
            watermark_settings.emit_query_interval_kind);

        watermark_settings.mode = WatermarkSettings::EmitMode::PERIODIC;
    }
    else if (emit->delay_interval)
    {
        extractInterval(
            emit->delay_interval->as<ASTFunction>(), watermark_settings.emit_query_interval, watermark_settings.emit_query_interval_kind);

        watermark_settings.mode
            = emit->after_watermark ? WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY : WatermarkSettings::EmitMode::DELAY;
    }
    else if (emit->after_watermark)
    {
        watermark_settings.mode = WatermarkSettings::EmitMode::WATERMARK;
    }
    else
        watermark_settings.mode = WatermarkSettings::EmitMode::NONE;

    if (emit->timeout_interval)
    {
        extractInterval(
            emit->timeout_interval->as<ASTFunction>(),
            watermark_settings.emit_timeout_interval,
            watermark_settings.emit_timeout_interval_kind);
    }
}
}

WatermarkSettings::WatermarkSettings(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc)
{
    window_desc = std::move(desc);

    const auto * select_query = query->as<ASTSelectQuery>();
    assert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    if (syntax_analyzer_result->aggregates.empty() && !syntax_analyzer_result->has_group_by)
    {
        /// if there is no aggregation, we don't need project watermark
        if (mode != EmitMode::TAIL && mode != EmitMode::NONE)
            throw Exception("Streaming tail mode doesn't support any watermark or periodic emit policy", ErrorCodes::SYNTAX_ERROR);

        if (mode == EmitMode::TAIL && emit_query_interval != 0)
            throw Exception("Streaming tail mode doesn't support any watermark or periodic emit policy", ErrorCodes::SYNTAX_ERROR);

        mode = EmitMode::TAIL;
    }
    else
    {
        if (window_desc && window_desc->type != WindowType::NONE)
        {
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
    global_aggr = true;

    if (mode == EmitMode::NONE)
    {
        /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
        mode = EmitMode::PERIODIC;
        emit_query_interval = 2000;
        emit_query_interval_kind = IntervalKind::Millisecond;
    }
}

void Watermark::preProcess()
{
    if (watermark_settings.mode == WatermarkSettings::EmitMode::PERIODIC)
        watermark_ts = MonotonicMilliseconds::now();
}

void Watermark::process(Chunk & chunk)
{
    if (watermark_settings.mode == WatermarkSettings::EmitMode::TAIL)
        return;

    if (watermark_settings.global_aggr)
    {
        /// global aggr emitted by using wall clock time of the current server
        last_event_seen_ts = MonotonicMilliseconds::now();
        max_event_ts = MonotonicMilliseconds::now();
        assignWatermark(chunk);
        return;
    }

    if (chunk.hasRows())
        doProcess(chunk);

    /// If after filtering, block is empty, we handle idleness
    if (!chunk.hasRows())
        handleIdleness(chunk);
}

void Watermark::assignWatermark(Chunk & chunk)
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
            auto now = MonotonicMilliseconds::now();
            auto next_watermark_ts = addTime(
                watermark_ts, watermark_settings.emit_query_interval_kind, watermark_settings.emit_query_interval, DateLUT::instance(), 3);
            if (now >= next_watermark_ts)
            {
                auto chunk_ctx = chunk.getOrCreateChunkContext();
                chunk_ctx->setWatermark(max_event_ts, last_projected_watermark_ts);
                chunk.setChunkContext(std::move(chunk_ctx));

                last_projected_watermark_ts = max_event_ts;
                watermark_ts = now;
                LOG_DEBUG(log, "Periodic time={}, rows={}", max_event_ts, chunk.getNumRows());
            }
            break;
        }
        case WatermarkSettings::EmitMode::DELAY: {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case WatermarkSettings::EmitMode::WATERMARK:
            processWatermark(chunk);
            break;

        case WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY:
            processWatermarkWithDelay(chunk);
            break;
    }
}

void Watermark::handleIdleness(Chunk & chunk)
{
    switch (watermark_settings.mode)
    {
        case WatermarkSettings::EmitMode::NONE:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::TAIL:
            assert(0);
            break;
        case WatermarkSettings::EmitMode::PERIODIC:
            /// periodic only applies to global aggr which shall have no idleness
            assert(0);
            break;
        case WatermarkSettings::EmitMode::DELAY: {
            throw Exception("DELAY emit doesn't implement yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        case WatermarkSettings::EmitMode::WATERMARK:
            handleIdlenessWatermark(chunk);
            break;
        case WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY:
            handleIdlenessWatermarkWithDelay(chunk);
            break;
    }
}

VersionType Watermark::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType Watermark::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}

void Watermark::serialize(WriteBuffer & wb) const
{
    /// Watermark has its own version than WatermarkTransform
    writeIntBinary(getVersion(), wb);

    writeIntBinary(max_event_ts, wb);
    writeIntBinary(watermark_ts, wb);
    writeIntBinary(last_projected_watermark_ts, wb);
    writeIntBinary(last_event_seen_ts, wb);
    writeIntBinary(late_events, wb);
    writeIntBinary(last_logged_late_events, wb);
    writeIntBinary(last_logged_late_events_ts, wb);
}

void Watermark::deserialize(ReadBuffer & rb)
{
    version = 0;

    readIntBinary(*version, rb);
    readIntBinary(max_event_ts, rb);
    readIntBinary(watermark_ts, rb);
    readIntBinary(last_projected_watermark_ts, rb);
    readIntBinary(last_event_seen_ts, rb);
    readIntBinary(late_events, rb);
    readIntBinary(last_logged_late_events, rb);
    readIntBinary(last_logged_late_events_ts, rb);
}
}
}
