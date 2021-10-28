#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/IntervalKind.h>

class DateLUTImpl;

namespace DB
{
/**
 * WatermarkBlockInputStream projects watermark according to watermark strategies
 * by observing the events in its `input`.
 */

struct SelectQueryInfo;

class WatermarkBlockInputStream : public IBlockInputStream
{
public:
    WatermarkBlockInputStream(
        BlockInputStreamPtr input_, const SelectQueryInfo & query_info, const String & partition_key_, Poco::Logger * log_);

    ~WatermarkBlockInputStream() override = default;

    String getName() const override { return "WatermarkBlockInputStream"; }

    Block getHeader() const override { return input->getHeader(); }

    void readPrefix() override { input->readPrefix(); }

    void cancel(bool kill) override;

public:
    enum class EmitMode
    {
        PERIODIC,
        DELAY,
        WATERMARK,
        WATERMARK_WITH_DELAY,
    };

    struct EmitSettings
    {
        String func_name;

        String time_col_name;

        const DateLUTImpl * timezone = nullptr;

        EmitMode mode = EmitMode::PERIODIC;

        Int64 window_interval = 0;
        IntervalKind::Kind window_interval_kind = IntervalKind::Second;

        Int64 hop_interval = 0;
        IntervalKind::Kind hop_interval_kind = IntervalKind::Second;

        Int64 emit_query_interval = 0;
        IntervalKind::Kind emit_query_interval_kind = IntervalKind::Second;

        bool time_col_is_datetime64 = false;
        UInt32 scale = 0;

        bool streaming = false;
    };

private:
    void readPrefixImpl() override;
    Block readImpl() override;

    /// Calculate watermark and attach it to Block
    void emitWatermark(Block & block, Int64 shrinked_max_event_ts);
    void handleIdleness(Block & block);
    void initWatermark(const SelectQueryInfo & query_info);
    Int64 getWindowUpperBound(Int64 ts);

private:
    BlockInputStreamPtr input;

    String partition_key;

    Poco::Logger * log;

    EmitSettings emit_settings;

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
}
