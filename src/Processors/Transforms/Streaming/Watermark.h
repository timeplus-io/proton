#pragma once

#include <Core/Types.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Common/IntervalKind.h>

class DateLUTImpl;

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
struct WatermarkSettings
{
public:
    WatermarkSettings(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc);

    enum class EmitMode
    {
        NONE,
        TAIL,
        PERIODIC,
        DELAY,
        WATERMARK,
        WATERMARK_WITH_DELAY,
    };

    const DateLUTImpl * timezone = nullptr;

    EmitMode mode = EmitMode::NONE;

    Int64 emit_query_interval = 0;
    IntervalKind::Kind emit_query_interval_kind = IntervalKind::Second;

    bool streaming = false;
    bool global_aggr = false;

    Int64 emit_timeout_interval = 0;
    IntervalKind::Kind emit_timeout_interval_kind = IntervalKind::Second;

    bool isTumbleWindowAggr() const { return window_desc && window_desc->type == WindowType::TUMBLE && mode != EmitMode::TAIL; }
    bool isHopWindowAggr() const { return window_desc && window_desc->type == WindowType::HOP && mode != EmitMode::TAIL;}

    /// privates
    FunctionDescriptionPtr window_desc;

private:
    void initWatermarkForGlobalAggr();
};

class Watermark
{
public:
    Watermark(WatermarkSettings watermark_settings_, bool proc_time_, Poco::Logger * log_)
        : watermark_settings(std::move(watermark_settings_)), proc_time(proc_time_), log(log_)
    {
    }
    Watermark(const Watermark &) = default;
    virtual ~Watermark() { }

    virtual std::unique_ptr<Watermark> clone() const { return std::make_unique<Watermark>(*this); }

    virtual String getName() const { return "Watermark"; }

    void preProcess();
    void process(Chunk & chunk);

    VersionType getVersion() const;

    virtual void serialize(WriteBuffer & wb) const;
    virtual void deserialize(ReadBuffer & rb);

protected:
    virtual void doProcess(Chunk & /* chunk*/) { }
    void assignWatermark(Chunk & chunk);
    virtual VersionType getVersionFromRevision(UInt64 revision) const;

private:
    /// EMIT STREAM AFTER WATERMARK
    virtual void processWatermarkWithDelay(Chunk & /* chunk */) { }

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    virtual void processWatermark(Chunk & /* chunk */) { }

    void handleIdleness(Chunk & chunk);

    virtual void handleIdlenessWatermark(Chunk & /* chunk */) { }

    virtual void handleIdlenessWatermarkWithDelay(Chunk & /* chunk */) { }

protected:
    WatermarkSettings watermark_settings;

    bool proc_time = false;

    Poco::Logger * log;

    mutable std::optional<VersionType> version;

    /// max event time observed so far
    Int64 max_event_ts = 0;

    /// max watermark projected so far
    Int64 watermark_ts = 0;
    Int64 last_projected_watermark_ts = 0;

    /// used for idle source
    Int64 last_event_seen_ts = 0;

    /// Event count which is late than current watermark
    UInt64 late_events = 0;
    UInt64 last_logged_late_events = 0;
    Int64 last_logged_late_events_ts = 0;
};

using WatermarkPtr = std::unique_ptr<Watermark>;
}
}
