#pragma once

#include <Core/Types.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <base/ClockUtils.h>
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

    WatermarkStrategy strategy = WatermarkStrategy::Unknown;

    WatermarkEmitMode mode = WatermarkEmitMode::Unknown;

    WindowInterval periodic_interval;

    /// With timeout
    WindowInterval timeout_interval;

    /// With delay
    WindowInterval delay_interval;
};

using WatermarkStamperParamsPtr = std::shared_ptr<WatermarkStamperParams>;

SERDE class WatermarkStamper final
{
public:
    WatermarkStamper(WatermarkStamperParams & params_, Poco::Logger * log_) : params(params_), log(log_) { }
    WatermarkStamper(const WatermarkStamper &) = default;
    ~WatermarkStamper() { }

    std::unique_ptr<WatermarkStamper> clone() const { return std::make_unique<WatermarkStamper>(*this); }

    String getDescription() const;

    void preProcess(const Block & header);

    void process(Chunk & chunk);

    /// During mute watermark, we still need to process the chunk to update max_event_ts
    void processWithMutedWatermark(Chunk & chunk);

    void processAfterUnmuted(Chunk & chunk);

    bool requiresPeriodicOrTimeoutEmit() const { return params.periodic_interval || params.timeout_interval; }

    VersionType getVersion() const;

    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

protected:
    VersionType getVersionFromRevision(UInt64 revision) const;

    /// \brief Process events and apply watermark
    void processWatermark(Chunk & chunk);

    /// \brief Emit a watermark according to the EmitMode
    /// \p old_watermark_ts - saved watermark before current processing
    /// \p has_events - whether there are events in the chunk, cannot use `hasRows()` of \p chunk since it's a filtered chunk
    void emitWatermark(Chunk & chunk, Int64 old_watermark_ts, bool has_events);

    /// \brief Generate and apply a watermark based on \p event_ts according to the WatermarkStrategy
    /// \return whether new watermark is applied
    bool applyWatermark(Int64 event_ts);

private:
    template <typename TimeColumnType, WatermarkStrategy strategy>
    void processWatermarkImpl(Chunk & chunk);

    template <WatermarkStrategy strategy>
    bool applyWatermarkImpl(Int64 event_ts);

    void logLateEvents();

protected:
    WatermarkStamperParams & params;
    Poco::Logger * log;

    ssize_t time_col_pos = -1;

    class InternalTimer
    {
    public:
        bool enabled() const { return interval != 0; }

        void setInterval(Int64 interval_) { interval = interval_; }

        void reset() { next_timeout = MonotonicNanoseconds::now() + interval; }

        /// \return: reset timepoint, 0: not expired, >0: expired
        Int64 resetIfExpired()
        {
            auto now = MonotonicNanoseconds::now();
            if (now >= next_timeout)
            {
                next_timeout = now + interval;
                return now;
            }
            return 0;
        }

    private:
        Int64 next_timeout = 0;
        Int64 interval = 0;
    };

    /// For periodic or timeout timer
    InternalTimer periodic_timer;
    InternalTimer timeout_timer;

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
    SERDE Int64 last_logged_late_events_ts = INVALID_WATERMARK;
};

using WatermarkStamperPtr = std::unique_ptr<WatermarkStamper>;
}
}
