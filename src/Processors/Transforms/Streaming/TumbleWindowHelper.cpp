#include <Processors/Transforms/Streaming/TumbleWindowHelper.h>

#include <Core/Streaming/Watermark.h>

namespace DB
{
namespace Streaming::TumbleWindowHelper
{
/// \brief Get max window can be finalized by the \param watermark
/// For example: tumble(<stream>, 3s), assume current watermark is `5s` so
/// - The window interval is `3s`
///  [second-0] [second-1] [second-2] [second-3] [second-4] [second-5] [second-6]
/// |           window-1             |                       ^
///                                  |            window-2   |        |
///                                                          |
///                                                     watermark (5s)
/// As above, we can finlize the follows windows:
/// 1) `window-1`, which contains `second-0`, `second-1`, `second-2`
Window getLastFinalizedWindow(Int64 watermark, const TumbleWindowParams & params)
{
    /// `last_finalized_window_end <= watermark`
    auto current_window_start = toStartTime(watermark, params.interval_kind, params.window_interval, *params.time_zone, params.time_scale);
    if (current_window_start > watermark) [[unlikely]]
        return {INVALID_WATERMARK, INVALID_WATERMARK}; /// No window

    return {
        addTime(current_window_start, params.interval_kind, -params.window_interval, *params.time_zone, params.time_scale),
        current_window_start};
}

/// \brief Get max exprired time bucket can be remove by the \param watermark
/// For example: tumble(<stream>, 3s), assume current watermark is `5s` so
/// - The window interval is `3s`
///  [second-0] [second-1] [second-2] [second-3] [second-4] [second-5] [second-6]
/// |           window-1             |                       ^
///                                  |            window-2   |        |
///                                                          |
///                                                     watermark (5s)
/// As above, we can finlize the follows windows:
/// 1) `window-1`, which contains `second-0`, `second-1`, `second-2`
/// So we can remove max window `window-1` (before next window-2)
Int64 getLastExpiredTimeBucket(Int64 watermark, const TumbleWindowParams & params, bool is_start_time_bucket)
{
    auto last_finalized_window = getLastFinalizedWindow(watermark, params);
    if (!last_finalized_window.isValid()) [[unlikely]]
        return watermark - 1;

    return is_start_time_bucket ? last_finalized_window.start : last_finalized_window.end;
}

WindowsWithBuckets getWindowsWithBuckets(const TumbleWindowParams & params, bool is_start_time_bucket, BucketsGetter buckets_getter)
{
    /// Get final buckets
    const auto & buckets = buckets_getter();
    if (buckets.empty())
        return {};

    WindowsWithBuckets windows_with_buckets;
    windows_with_buckets.reserve(buckets.size());
    if (is_start_time_bucket)
    {
        for (auto time_bucket : buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {static_cast<Int64>(time_bucket),
                 addTime(
                     static_cast<Int64>(time_bucket), params.interval_kind, params.window_interval, *params.time_zone, params.time_scale)},
                {time_bucket}});
    }
    else
    {
        for (auto time_bucket : buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {addTime(
                     static_cast<Int64>(time_bucket), params.interval_kind, -params.window_interval, *params.time_zone, params.time_scale),
                 static_cast<Int64>(time_bucket)},
                {time_bucket}});
    }

    return windows_with_buckets;
}
}
}
