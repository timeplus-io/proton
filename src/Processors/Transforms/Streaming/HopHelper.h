#pragma once

#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
namespace Streaming::HopHelper
{
WindowInterval gcdWindowInterval(const ColumnWithTypeAndName & interval_col1, const ColumnWithTypeAndName & interval_col2);

/// @brief Get max window can be finalized by the @param watermark
Window getLastFinalizedWindow(Int64 watermark, const HopWindowParams & params);

/// @brief Get max exprired time bucket can be remove by the @param watermark
/// @param is_start_time_bucket. true: <gcd window start time>, otherwise: <gcd window end time>
size_t getLastExpiredTimeBucket(Int64 watermark, const HopWindowParams & params, bool is_start_time_bucket);
}
}
