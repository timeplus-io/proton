#include <Processors/Transforms/Streaming/HopHelper.h>
#include <Processors/Transforms/Streaming/HopWatermarkStamper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
HopWatermarkStamper::HopWatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_)
    : WatermarkStamper(params_, log_), window_params(params.window_params->as<HopWindowParams &>())
{
    if (params.mode != WatermarkStamperParams::EmitMode::WATERMARK && params.mode != WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW
        && params.mode != WatermarkStamperParams::EmitMode::TAIL)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} doesn't support emit mode '{}'", getName(), magic_enum::enum_name(params.mode));
}

Int64 HopWatermarkStamper::calculateWatermarkImpl(Int64 event_ts) const
{
    auto last_finalized_window = HopHelper::getLastFinalizedWindow(event_ts, window_params);
    if (likely(last_finalized_window.isValid()))
        return last_finalized_window.end;
    else
        return event_ts;
}

}
}
