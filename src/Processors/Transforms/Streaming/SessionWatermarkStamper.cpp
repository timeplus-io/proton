#include <Processors/Transforms/Streaming/SessionWatermarkStamper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
SessionWatermarkStamper::SessionWatermarkStamper(WatermarkStamperParams && params_, Poco::Logger * log_)
    : WatermarkStamper(std::move(params_), log_), window_params(params.window_params->as<SessionWindowParams &>())
{
    if (params.mode != WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW && params.mode != WatermarkStamperParams::EmitMode::TAIL)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} doesn't support emit mode '{}'", getName(), magic_enum::enum_name(params.mode));
}

}
}
