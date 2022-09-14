#pragma once

#include "Watermark.h"

namespace DB
{
namespace Streaming
{
class HopTumbleBaseWatermark : public Watermark
{
public:
    explicit HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, bool proc_time_, Poco::Logger * log_);
    HopTumbleBaseWatermark(const HopTumbleBaseWatermark &) = default;
    ~HopTumbleBaseWatermark() override = default;

protected:
    void init(Int64 & interval);
    void initTimezone(size_t timezone_pos);

private:
    void doProcess(Block & block) override;

    /// EMIT STREAM AFTER WATERMARK
    void processWatermark(Block & block) override;

    /// EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL <n> <UNIT>
    void processWatermarkWithDelay(Block & block) override;

    void handleIdlenessWatermark(Block & block) override;

    void handleIdlenessWatermarkWithDelay(Block & block) override;

    virtual Int64 getProgressingInterval() const { return window_interval; }

    std::pair<Int64, Int64> initFirstWindow(Block & block, bool delay = false) const;
    std::pair<Int64, Int64> initFirstWindowWithAutoScale(Block & block, bool delay = false) const;
    std::pair<Int64, Int64> doInitFirstWindow(Block & block, bool delay = false) const;

    std::pair<Int64, Int64> getWindow(Int64 time_sec) const;

    Int64 addTimeWithAutoScale(Int64 datetime64, IntervalKind::Kind kind, Int64 interval);

    void processWatermarkWithAutoScale(Block & block);
    void doProcessWatermark(Block & block);

    void processWatermarkWithDelayAndWithAutoScale(Block & block);
    void doProcessWatermarkWithDelay(Block & block);

protected:
    Int64 window_interval = 0;
    IntervalKind::Kind window_interval_kind = IntervalKind::Second;

    Int32 scale = 0;
    Int64 multiplier = 1;
    bool time_col_is_datetime64 = false;

    String time_col_name;

    const DateLUTImpl * timezone = nullptr;

    bool has_event_in_window = false;
};

}
}
