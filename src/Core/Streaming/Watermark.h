#pragma once

#include <base/types.h>

namespace DB::Streaming
{
/// For DateTime64 (i.e. Int64), supported range of values:
/// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
/// The 1970-01-01 00:00:00 is Int64 value 0, earlier times are negative, so we need to use signed Int64 as watermark
constexpr Int64 INVALID_WATERMARK = std::numeric_limits<Int64>::min();
constexpr Int64 MIN_WATERMARK = std::numeric_limits<Int64>::min() + 1;
constexpr Int64 TIMEOUT_WATERMARK = std::numeric_limits<Int64>::max();

enum class EmitMode : uint8_t
{
    None = 0,

    Tail,

    AfterKeyExpire, /// Emit when either max time span reaches since seeing a key or timeout reaches when there is no new data for a key

    Periodic, /// Emit results periodically

    AfterWindowClose, /// Emit results after window close (Only for window aggr)

    PerEvent, /// Emit results per event

    OnUpdate = 1u << 7, /// Emit results on update
    OnUpdateWithBatchInterval, /// Emit results on update with batch interval
};
}
