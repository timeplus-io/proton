#pragma once

#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
struct WatermarkBound;

namespace Streaming::HopHelper
{
std::pair<Int64, Int64> getLastFinalizedWindow(Int64 watermark, const HopWindowParams & params);
}
}
