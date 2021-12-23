#include "HopWatermark.h"

#include <Parsers/ASTFunction.h>
#include <base/ClockUtils.h>
#include <base/logger_useful.h>

namespace DB
{
HopWatermark::HopWatermark(WatermarkSettings && watermark_settings_, bool proc_time_, Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), proc_time_, log_)
{
    HopTumbleBaseWatermark::init(hop_interval);

    auto * func_ast = watermark_settings.window_desc->func_ast->as<ASTFunction>();
    extractInterval(
        func_ast->arguments->children[2]->as<ASTFunction>(),
        window_interval,
        window_interval_kind);

    initTimezone(3);
}
}
