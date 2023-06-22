#include <Processors/Transforms/Streaming/TumbleWatermarkStamper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
}
namespace Streaming
{
TumbleWatermarkStamper::TumbleWatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_)
    : WatermarkStamper(params_, log_), window_params(params.window_params->as<TumbleWindowParams &>())
{
    if (params.mode != WatermarkStamperParams::EmitMode::WATERMARK && params.mode != WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW
        && params.mode != WatermarkStamperParams::EmitMode::TAIL)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} doesn't support emit mode '{}'", getName(), magic_enum::enum_name(params.mode));
}

Int64 TumbleWatermarkStamper::calculateWatermark(Int64 event_ts) const
{
    return toStartTime(
        event_ts, window_params.interval_kind, window_params.window_interval, *window_params.time_zone, window_params.time_scale);
}

}
}
