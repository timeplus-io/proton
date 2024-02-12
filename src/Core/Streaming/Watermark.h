#pragma once

#include <base/types.h>

namespace DB
{
namespace Streaming
{
/// For DateTime64 (i.e. Int64), supported range of values:
/// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
/// The 1970-01-01 00:00:00 is Int64 value 0, earlier times are negative, so we need to use signed Int64 as watermark
constexpr Int64 INVALID_WATERMARK = std::numeric_limits<Int64>::min();
constexpr Int64 TIMEOUT_WATERMARK = std::numeric_limits<Int64>::max();

/// NOTE: requires highest bit to be 1
constexpr uint8_t EMIT_UPDATES_MASK = 1u << 7;

/// TODO: Separate EmitMode into two parts:
/// 1) EmitStrategy         - how to do finalization (what to emit) for all aggregates processing
/// 2) WatermarkStragegy    - how / when to stamp watermark
enum class EmitMode : uint8_t
{
    None = 0,
    Tail,

    Periodic,  /// Emit a processing time watermark at periodic interval

    Watermark, /// Allow time skew in same window, emit the watermark when a window closed
    PeriodicWatermark, /// Same as WATERMARK, but emit the watermark at periodic interval

    /* emit only keyed and changed states for aggregating */
    OnUpdate = EMIT_UPDATES_MASK, /// Emit a processing time watermark per batch of events
    PeriodicOnUpdate, /// Same as Periodic
    WatermarkOnUpdate, /// Same as Watermark, but emit the watermark per batch of events
    PeriodicWatermarkOnUpdate, /// Same as PeriodicWatermark
};
}
}
