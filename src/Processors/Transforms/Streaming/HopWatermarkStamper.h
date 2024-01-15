#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

namespace DB
{
namespace Streaming
{
class HopWatermarkStamper final : public WatermarkStamper
{
public:
    HopWatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_);
    HopWatermarkStamper(const HopWatermarkStamper &) = default;
    ~HopWatermarkStamper() override = default;

    String getName() const override { return "HopWatermarkStamper"; }
    WatermarkStamperPtr clone() const override { return std::make_unique<HopWatermarkStamper>(*this); }

private:
    Int64 calculateWatermarkImpl(Int64 event_ts) const override;

    HopWindowParams & window_params;
};

}
}
