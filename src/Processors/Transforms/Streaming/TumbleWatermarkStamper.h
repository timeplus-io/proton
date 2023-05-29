#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

namespace DB
{
namespace Streaming
{
class TumbleWatermarkStamper final : public WatermarkStamper
{
public:
    TumbleWatermarkStamper(WatermarkStamperParams && params_, Poco::Logger * log_);
    TumbleWatermarkStamper(const TumbleWatermarkStamper &) = default;
    ~TumbleWatermarkStamper() override = default;

    String getName() const override { return "TumbleWatermarkStamper"; }

    WatermarkStamperPtr clone() const override { return std::make_unique<TumbleWatermarkStamper>(*this); }

private:
    Int64 calculateWatermark(Int64 event_ts) const override;

    TumbleWindowParams & window_params;
};

}
}
