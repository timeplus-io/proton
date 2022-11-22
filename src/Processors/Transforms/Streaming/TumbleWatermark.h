#pragma once

#include "HopTumbleBaseWatermark.h"

namespace DB
{
namespace Streaming
{
class TumbleWatermark : public HopTumbleBaseWatermark
{
public:
    explicit TumbleWatermark(WatermarkSettings && watermark_settings_, size_t time_col_position_, bool proc_time_, Poco::Logger * log_);
    TumbleWatermark(const TumbleWatermark &) = default;
    ~TumbleWatermark() override = default;

    String getName() const override { return "TumbleWatermark"; }

    WatermarkPtr clone() const override { return std::make_unique<TumbleWatermark>(*this); }
};
}
}
