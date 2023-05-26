#include <Processors/Transforms/Streaming/HopHelper.h>

namespace DB
{
namespace Streaming::HopHelper
{
WindowInterval gcdWindowInterval(const ColumnWithTypeAndName & interval_col1, const ColumnWithTypeAndName & interval_col2)
{
    auto interval1 = extractInterval(interval_col1);
    auto interval2 = extractInterval(interval_col2);
    assert(interval1.unit == interval2.unit);
    return {std::gcd(interval1.interval, interval2.interval), interval1.unit};
}

/// @brief Get max window can be finalized by the @param watermark
/// For example: hop(<stream>, 2s, 3s), assume current watermark is `6s` so
/// - The slide interval is `2s`
/// - The window interval is `3s`
/// - The base-bucket interval is `1s`
///  [bucket-0] [bucket-1] [bucket-2] [bucket-3] [bucket-4] [bucket-5] [bucket-6]
/// |           window-1             |                                  ^
///                       |            window-2            |            |
///                                             |            window-3   |        |
///                                                                     |
///                                                               watermark (6s)
/// As above, we can finlize the follows windows:
/// 1) `window-1`, which contains `bucket-0`, `bucket-1`, `bucket-2`
/// 2) `window-2`, which contains `bucket-2`, `bucket-3`, `bucket-4`
Window getLastFinalizedWindow(Int64 watermark, const HopWindowParams & params)
{
    if (unlikely(watermark <= 0))
        return {}; /// No window

    /// `last_finalized_window_end <= watermark`
    auto current_window_start = toStartTime(watermark, params.interval_kind, params.slide_interval, *params.time_zone, params.time_scale);
    auto last_finalized_window_end
        = addTime(current_window_start, params.interval_kind, params.window_interval, *params.time_zone, params.time_scale);

    while (last_finalized_window_end > watermark)
        last_finalized_window_end
            = addTime(last_finalized_window_end, params.interval_kind, -params.slide_interval, *params.time_zone, params.time_scale);

    return {
        addTime(last_finalized_window_end, params.interval_kind, -params.window_interval, *params.time_zone, params.time_scale),
        last_finalized_window_end};
}

/// @brief Get max exprired time bucket can be remove by the @param watermark
/// @param is_start_time_bucket. true: <gcd window start time>, otherwise: <gcd window end time>
/// For example: hop(<stream>, 2s, 3s), assume current watermark is `6s` so
/// - The slide interval is `2s`
/// - The window interval is `3s`
/// - The base-bucket interval is `1s`
///  [bucket-0] [bucket-1] [bucket-2] [bucket-3] [bucket-4] [bucket-5] [bucket-6]
/// |           window-1             |                                  ^
///                       |            window-2            |            |
///                                             |            window-3   |        |
///                                                                     |
///                                                               watermark (6s)
/// As above, we finlized the follows windows:
/// 1) `window-1`, which contains `bucket-0`, `bucket-1`, `bucket-2`
/// 2) `window-2`, which contains `bucket-2`, `bucket-3`, `bucket-4`
/// So we can remove max bucket `bucket-3` (before next window-3)
size_t getLastExpiredTimeBucket(Int64 watermark, const HopWindowParams & params, bool is_start_time_bucket)
{
    auto [last_finalized_window_start, last_finalized_window_end] = getLastFinalizedWindow(watermark, params);

    /// `expired_buckets < min_bucket_of_next_finalized_window`
    /// => max_expired_bucket = min_bucket_of_next_finalized_window - gcd_interval
    if (is_start_time_bucket)
        /// `min_bucket_of_next_finalized_window = next_finalized_window_start`
        /// => max_expired_bucket = min_bucket_of_next_finalized_window - gcd_interval`
        ///                              = (next_finalized_window_start) - gcd_interval
        ///                              = (last_finalized_window_start + slide_interval) - gcd_interval
        return addTime(
            last_finalized_window_start,
            params.interval_kind,
            params.slide_interval - params.gcd_interval,
            *params.time_zone,
            params.time_scale);
    else
        /// `min_bucket_of_next_finalized_window = next_finalized_window_start + gcd_interval`
        /// => max_expired_bucket = min_bucket_of_next_finalized_window - gcd_interval
        ///                              = (next_finalized_window_start + gcd_interval) - gcd_interval
        ///                              = next_finalized_window_start
        ///                              = (last_finalized_window_start + slide_interval)
        return addTime(last_finalized_window_start, params.interval_kind, params.slide_interval, *params.time_zone, params.time_scale);
}
}
}
