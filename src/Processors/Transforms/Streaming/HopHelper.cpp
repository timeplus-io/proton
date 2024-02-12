#include <Processors/Transforms/Streaming/HopHelper.h>

#include <Core/Streaming/Watermark.h>

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
    /// `last_finalized_window_end <= watermark`
    auto current_window_start = toStartTime(watermark, params.interval_kind, params.slide_interval, *params.time_zone, params.time_scale);
    if (current_window_start > watermark) [[unlikely]]
        return {INVALID_WATERMARK, INVALID_WATERMARK}; /// No window

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
Int64 getLastExpiredTimeBucket(Int64 watermark, const HopWindowParams & params, bool is_start_time_bucket)
{
    auto last_finalized_window = getLastFinalizedWindow(watermark, params);
    if (unlikely(!last_finalized_window.isValid()))
        return watermark - 1;

    /// `expired_buckets < min_bucket_of_next_finalized_window`
    /// => max_expired_bucket = min_bucket_of_next_finalized_window - gcd_interval
    if (is_start_time_bucket)
        /// `min_bucket_of_next_finalized_window = next_finalized_window_start`
        /// => max_expired_bucket = min_bucket_of_next_finalized_window - gcd_interval`
        ///                              = (next_finalized_window_start) - gcd_interval
        ///                              = (last_finalized_window_start + slide_interval) - gcd_interval
        return addTime(
            last_finalized_window.start,
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
        return addTime(last_finalized_window.start, params.interval_kind, params.slide_interval, *params.time_zone, params.time_scale);
}

WindowsWithBuckets getWindowsWithBuckets(const HopWindowParams & params, bool is_start_time_bucket, BucketsGetter buckets_getter)
{
    /// Get final buckets
    const auto & buckets = buckets_getter();
    if (buckets.empty())
        return {};

    Window window;
    Int64 min_bucket_of_window, max_bucket_of_window;
    auto calc_window_min_max_buckets = [&]() {
        if (is_start_time_bucket)
        {
            min_bucket_of_window = window.start;
            max_bucket_of_window = addTime(window.end, params.interval_kind, -params.gcd_interval, *params.time_zone, params.time_scale);
        }
        else
        {
            min_bucket_of_window = addTime(window.start, params.interval_kind, params.gcd_interval, *params.time_zone, params.time_scale);
            max_bucket_of_window = window.end;
        }
    };

    /// Initial first window
    window.start = toStartTime(
        is_start_time_bucket ? buckets.back() : buckets.back() - 1,
        params.interval_kind,
        params.slide_interval,
        *params.time_zone,
        params.time_scale);
    window.end = addTime(window.start, params.interval_kind, params.window_interval, *params.time_zone, params.time_scale);
    if (unlikely(!window.isValid()))
        return {};

    calc_window_min_max_buckets();

    /// Collect windows
    WindowsWithBuckets windows_with_buckets;
    while (*buckets.begin() <= max_bucket_of_window)
    {
        auto window_with_buckets = windows_with_buckets.emplace(windows_with_buckets.begin(), WindowWithBuckets{window, {}});
        for (auto time_bucket : buckets)
        {
            if (time_bucket >= min_bucket_of_window && time_bucket <= max_bucket_of_window)
                window_with_buckets->buckets.emplace_back(time_bucket);
        }

        if (unlikely(windows_with_buckets.front().buckets.empty()))
            windows_with_buckets.erase(windows_with_buckets.begin());

        /// Previous window
        window.start = addTime(window.start, params.interval_kind, -params.slide_interval, *params.time_zone, params.time_scale);
        window.end = addTime(window.end, params.interval_kind, -params.slide_interval, *params.time_zone, params.time_scale);

        if (unlikely(!window.isValid()))
            break;

        calc_window_min_max_buckets();
    }

    return windows_with_buckets;
}
}
}
