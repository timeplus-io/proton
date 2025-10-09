#pragma once

#include <Core/Streaming/Watermark.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Processors/Transforms/Streaming/EmitParams.h>
#include <Common/serde.h>
#include <Common/Logger.h>

namespace DB
{
struct SelectQueryInfo;
class Chunk;

namespace Streaming
{

SERDE class WatermarkStamper final
{
public:
    WatermarkStamper(const EmitParams & params_, LoggerPtr logger_) : params(params_), logger(logger_) { }
    WatermarkStamper(const WatermarkStamper &) = default;
    ~WatermarkStamper() = default;

    std::unique_ptr<WatermarkStamper> clone() const { return std::make_unique<WatermarkStamper>(*this); }

    String getName() const;

    void preProcess(const Block & header);

    void process(Chunk & chunk);

    /// During mute watermark, we still need to process the chunk to update max_event_ts
    void processWithMutedWatermark(Chunk & chunk);

    void processAfterUnmuted(Chunk & chunk);

    bool requiresPeriodicOrTimeoutEmit() const { return periodic_interval || timeout_interval; }

    VersionType getVersion() const;

    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

protected:
    VersionType getVersionFromRevision(UInt64 revision) const;

private:
    void processWatermark(Chunk & chunk);

    template <typename TimeColumnType, bool apply_watermark_per_row>
    void processWatermarkImpl(Chunk & chunk);

    void processPeriodic(Chunk & chunk);

    void processTimeout(Chunk & chunk);

    void logLateEvents();

    ALWAYS_INLINE Int64 calculateWatermark(Int64 event_ts) const;
    ALWAYS_INLINE Int64 calculateWatermarkPerRow(Int64 event_ts) const;

    void initPeriodicTimer(const WindowInterval & interval);

    void initTimeoutTimer(const WindowInterval & interval);

    bool useEventTime() const { return time_col_pos >= 0; }

protected:
    const EmitParams & params;
    LoggerPtr logger;

    ssize_t time_col_pos = -1;

    /// For periodic
    Int64 next_periodic_emit_ts = 0;
    Int64 periodic_interval = 0;

    /// For timeout
    Int64 next_timeout_emit_ts = 0;
    Int64 timeout_interval = 0;

    /// (State)
    SERDE mutable std::optional<VersionType> version;

    /// max event time observed so far
    SERDE Int64 max_event_ts = INVALID_WATERMARK;

    /// max watermark projected so far.
    /// Not initialize to INVALID_WATERMARK, we can dirctly emit it periodically if no watermark is applied
    SERDE Int64 watermark_ts = INVALID_WATERMARK + 1;

    /// Event count which is late than current watermark
    static constexpr Int64 LOG_LATE_EVENTS_INTERVAL_SECONDS = 5; /// 5s, TODO: add settings ?
    SERDE UInt64 late_events = 0;
    SERDE UInt64 last_logged_late_events = 0;
    SERDE Int64 last_logged_late_events_ts = 0;
};

using WatermarkStamperPtr = std::unique_ptr<WatermarkStamper>;
}
}
