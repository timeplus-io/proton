#include <Processors/Transforms/Streaming/HopHelper.h>

namespace DB
{
namespace Streaming::HopHelper
{
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
/// As above, we can project some windows:
/// 1) `window-1`, which contains `bucket-0`, `bucket-1`, `bucket-2`
/// 2) `window-2`, which contains `bucket-2`, `bucket-3`, `bucket-4`
std::pair<Int64, Int64> getLastFinalizedWindow(Int64 watermark, const HopWindowParams & params)
{
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
}
}
