#pragma once

#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
namespace Streaming
{
enum class WatermarkStrategy;
enum class WatermarkEmitMode;

namespace TumbleHelper
{
/// @brief Get max window can be finalized by the @param watermark
Window getLastFinalizedWindow(Int64 watermark, const TumbleWindowParams & params);

/// @brief Get max exprired time bucket can be remove by the @param watermark
/// @param is_start_time_bucket. true: <window start time>, false: <window end time>
Int64 getLastExpiredTimeBucket(Int64 watermark, const TumbleWindowParams & params, bool is_start_time_bucket);

/// @brief Get windows with their associated buckets.
using BucketsGetter = std::function<std::vector<Int64>()>;
WindowsWithBuckets getWindowsWithBuckets(const TumbleWindowParams & params, bool is_start_time_bucket, BucketsGetter buckets_getter);

/// @brief Validate watermark strategy and emit mode for tumble window
void validateWatermarkStrategyAndEmitMode(WatermarkStrategy & strategy, WatermarkEmitMode & mode, TumbleWindowParams & params);
}
}
}
