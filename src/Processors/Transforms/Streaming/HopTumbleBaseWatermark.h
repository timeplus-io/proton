#pragma once

#include "Watermark.h"

namespace DB
{
namespace Streaming
{
class HopTumbleBaseWatermark : public Watermark
{
public:
    explicit HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, size_t time_col_position_, bool proc_time_, Poco::Logger * log_);
    HopTumbleBaseWatermark(const HopTumbleBaseWatermark &) = default;
    ~HopTumbleBaseWatermark() override = default;

    void serialize(WriteBuffer & wb) const override;
    void deserialize(ReadBuffer & rb) override;

protected:
    void init(Int64 & interval);
    void initTimezone(size_t timezone_pos);

private:
    void doProcess(Chunk & chunk) override;

    /// EMIT STREAM AFTER WATERMARK
    void processWatermark(Chunk & chunk) override;

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    void processWatermarkWithDelay(Chunk & chunk) override;

    void handleIdlenessWatermark(Chunk & chunk) override;

    void handleIdlenessWatermarkWithDelay(Chunk & chunk) override;

    virtual Int64 getProgressingInterval() const { return window_interval; }

    std::pair<Int64, Int64> initFirstWindow(Chunk & chunk, bool delay = false) const;
    std::pair<Int64, Int64> initFirstWindowWithAutoScale(Chunk & chunk, bool delay = false) const;
    std::pair<Int64, Int64> doInitFirstWindow(Chunk & chunk, bool delay = false) const;

    std::pair<Int64, Int64> getWindow(Int64 timestamp) const;

protected:
    Int64 window_interval = 0;
    IntervalKind::Kind window_interval_kind = IntervalKind::Second;

    UInt16 time_col_position;
    bool time_col_is_datetime64 = false;

    Int32 scale = 0;
    Int64 multiplier = 1;

    const DateLUTImpl * timezone = nullptr;

    bool has_event_in_window = false;
};

}
}
