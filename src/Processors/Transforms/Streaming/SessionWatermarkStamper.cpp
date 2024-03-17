#include <Processors/Transforms/Streaming/SessionWatermarkStamper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
SessionWatermarkStamper::SessionWatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_)
    : WatermarkStamper(params_, log_), window_params(params.window_params->as<SessionWindowParams &>())
{
}

}
}
