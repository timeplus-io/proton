#pragma once

#include <Core/Types.h>
#include <Interpreters/StreamingFunctionDescription.h>
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
class Block;

struct WatermarkSettings
{
public:
    explicit WatermarkSettings(const SelectQueryInfo & query_info, StreamingFunctionDescriptionPtr desc_);

    enum class EmitMode
    {
        NONE,
        TAIL,
        PERIODIC,
        DELAY,
        WATERMARK,
        WATERMARK_WITH_DELAY,
    };

    String func_name;

    const DateLUTImpl * timezone = nullptr;

    EmitMode mode = EmitMode::NONE;

    Int64 emit_query_interval = 0;
    IntervalKind::Kind emit_query_interval_kind = IntervalKind::Second;

    bool streaming = false;
    bool global_aggr = false;

    /// privates
    StreamingFunctionDescriptionPtr window_desc;

private:
    void initWatermarkForGlobalAggr();
};

class Watermark
{
public:
    Watermark(WatermarkSettings && watermark_settings_, const String & partition_key_, Poco::Logger * log_)
        : watermark_settings(std::move(watermark_settings_)), partition_key(partition_key_), log(log_)
    {
    }
    virtual ~Watermark() { }

    void preProcess();
    void process(Block & block);

protected:
    virtual void doProcess(Block & /* block */) { }
    void assignWatermark(Block & block, Int64 max_event_ts_secs);

private:
    /// EMIT STREAM AFTER WATERMARK
    virtual void processWatermarkWithDelay(Block & /* block */, Int64 /* max_event_ts_secs */) { }

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    virtual void processWatermark(Block & /* block */, Int64 /* max_event_ts_secs */) { }

    void handleIdleness(Block & block);

    virtual void handleIdlenessWatermark(Block & /* block */) { }

    virtual void handleIdlenessWatermarkWithDelay(Block & /* block */) { }

protected:
    WatermarkSettings watermark_settings;

    String partition_key;

    Poco::Logger * log;

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

void extractInterval(ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind);
Int64 addTime(Int64 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone);

using WatermarkPtr = std::shared_ptr<Watermark>;
}
