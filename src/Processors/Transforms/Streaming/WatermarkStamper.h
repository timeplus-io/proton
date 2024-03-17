#pragma once

#include <Core/Types.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <Common/serde.h>

namespace Poco
{
class Logger;
}

namespace DB
{
struct SelectQueryInfo;
class Chunk;

namespace Streaming
{
struct WatermarkStamperParams
{
public:
    WatermarkStamperParams(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, WindowParamsPtr window_params_);

    WindowParamsPtr window_params;

    EmitMode mode = EmitMode::None;

    WindowInterval periodic_interval;

    /// With timeout
    WindowInterval timeout_interval;

    /// With delay
    WindowInterval delay_interval;
};

using WatermarkStamperParamsPtr = std::shared_ptr<const WatermarkStamperParams>;

SERDE class WatermarkStamper
{
public:
    WatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_) : params(params_), log(log_) { }
    WatermarkStamper(const WatermarkStamper &) = default;
    virtual ~WatermarkStamper() { }

    virtual std::unique_ptr<WatermarkStamper> clone() const { return std::make_unique<WatermarkStamper>(*this); }

    virtual String getName() const { return "WatermarkStamper"; }

    void preProcess(const Block & header);

    void process(Chunk & chunk);

    /// During mute watermark, we still need to process the chunk to update max_event_ts
    void processWithMutedWatermark(Chunk & chunk);

    void processAfterUnmuted(Chunk & chunk);

    bool requiresPeriodicOrTimeoutEmit() const { return periodic_interval || timeout_interval; }

    VersionType getVersion() const;

    virtual void serialize(WriteBuffer & wb) const;
    virtual void deserialize(ReadBuffer & rb);

protected:
    virtual VersionType getVersionFromRevision(UInt64 revision) const;

private:
    void processWatermark(Chunk & chunk);

    template <typename TimeColumnType, bool apply_watermark_per_row>
    void processWatermarkImpl(Chunk & chunk);

    /// \param use_processing_time - if true, use processing time as watermark, otherwise use event time `watermark_ts`
    void processPeriodic(Chunk & chunk, bool use_processing_time);

    void processTimeout(Chunk & chunk);

    void logLateEvents();

    ALWAYS_INLINE Int64 calculateWatermark(Int64 event_ts) const;
    ALWAYS_INLINE Int64 calculateWatermarkPerRow(Int64 event_ts) const;

    virtual Int64 calculateWatermarkImpl(Int64 event_ts) const;

    void initPeriodicTimer(const WindowInterval & interval);

    void initTimeoutTimer(const WindowInterval & interval);

protected:
    const WatermarkStamperParams & params;
    Poco::Logger * log;

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
