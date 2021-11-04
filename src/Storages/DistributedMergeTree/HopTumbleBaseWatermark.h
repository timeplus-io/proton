#pragma once

#include "Watermark.h"

namespace DB
{
class HopTumbleBaseWatermark : public Watermark
{
public:
    explicit HopTumbleBaseWatermark(WatermarkSettings && watermark_settings_, const String & partition_key_, Poco::Logger * log_);
    ~HopTumbleBaseWatermark() override = default;

protected:
    void init(Int64 & interval);
    void initTimezone(size_t timezone_pos);

    void doProcess(Block & block) override;

protected:
    Int64 window_interval = 0;
    IntervalKind::Kind window_interval_kind = IntervalKind::Second;

    UInt32 scale = 0;
    bool time_col_is_datetime64 = false;

    String time_col_name;

    const DateLUTImpl * timezone = nullptr;
};
}
