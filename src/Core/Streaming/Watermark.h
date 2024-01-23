#pragma once

namespace DB
{
namespace Streaming
{
/// For DateTime64 (i.e. Int64), supported range of values:
/// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
/// The 1970-01-01 00:00:00 is Int64 value 0, earlier times are negative, so we need to use signed Int64 as watermark
constexpr Int64 INVALID_WATERMARK = std::numeric_limits<Int64>::min();
constexpr Int64 TIMEOUT_WATERMARK = std::numeric_limits<Int64>::max();


enum class WatermarkStrategy
{
    Unknown,

    /// No watermark
    None,

    /// Watermark is generated based on each event, this is used for `EMIT AFTER WATERMARK WITHOUT DELAY`
    /// - Not allow time skew
    Ascending,

    /// Watermark is generated based on last event with a delay interval, this is used for `EMIT AFTER WATERMARK WITH DELAY <delay_interval>`:
    /// - Allow time skew in a certain range `[<max_event_ts - delay_interval>, <max_event_ts>]`)
    BoundedOutOfOrderness,

    /// (Built-in): Watermark is generated based on a batch events:
    /// - Allow time skew in one batch events
    OutOfOrdernessInBatch,

    /// (Built-in): Watermark is generated based on new window of a batch events:
    /// - Allow time skew in same window and one batch
    OutOfOrdernessInWindowAndBatch,

    /// TODO: more strategies ...
};

enum class WatermarkEmitMode
{
    Unknown,

    None,                               /// Not emit any watermark

    /* Based on processing time */
    Periodic,                           /// Watermark is emitted at periodic interval
    PeriodicOnUpdate,                   /// Watermark is emitted at periodic interval (only emit aggregated result of groups with updates)

    /* Based on event time */
    OnWindow,                           /// Watermark is emitted when a window close
    OnUpdate,                           /// Watermark is emitted for every batch of events (only emit aggregated result of groups with updates)
};

}
}
