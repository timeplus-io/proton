#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

namespace DB
{
namespace Streaming
{
class SessionWatermarkStamper final : public WatermarkStamper
{
public:
    SessionWatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_);
    SessionWatermarkStamper(const SessionWatermarkStamper &) = default;
    ~SessionWatermarkStamper() override = default;

    String getName() const override { return "SessionWatermarkStamper"; }
    WatermarkStamperPtr clone() const override { return std::make_unique<SessionWatermarkStamper>(*this); }

private:
    SessionWindowParams & window_params;
};

}
}
