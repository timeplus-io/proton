#pragma once

#include "Watermark.h"

namespace DB
{
class HopTumbleBaseWatermark : public Watermark
{
public:
    explicit HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, Poco::Logger * log_);
    ~HopTumbleBaseWatermark() override = default;

protected:
    void init(Int64 & interval);
    void initTimezone(size_t timezone_pos);

    void doProcess(Block & block) override;

    /// EMIT STREAM AFTER WATERMARK
    void processWatermark(Block & block) override;

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    void processWatermarkWithDelay(Block & block) override;

    void handleIdlenessWatermark(Block & block) override;

    void handleIdlenessWatermarkWithDelay(Block & block) override;

    virtual Int64 getProgressingInterval() const { return window_interval; }

    std::pair<Int64, Int64> initFirstWindow() const;
    std::pair<Int64, Int64> getWindow(Int64 time_sec) const;

    Int64 addTimeWithAutoScale(Int64 datetime64, IntervalKind::Kind kind, Int64 interval);

protected:
    Int64 window_interval = 0;
    IntervalKind::Kind window_interval_kind = IntervalKind::Second;

    UInt32 scale = 0;
    bool time_col_is_datetime64 = false;

    String time_col_name;

    const DateLUTImpl * timezone = nullptr;
};
}
