#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/serde.h>

namespace DB
{
using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;

namespace Streaming
{
/// Tracking Key timestamps
SERDE struct TrackingTime
{
    void serialize(WriteBuffer & wb) const
    {
        writeVarUInt(min_ts, wb);
        writeVarUInt(max_ts, wb);
    }

    void deserialize(ReadBuffer & rb)
    {
        readVarUInt(min_ts, rb);
        readVarUInt(max_ts, rb);
    }

    static void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & wb)
    {
        const auto & data_ex = data(place);
        data_ex.serialize(wb);
    }

    static void deserialize(AggregateDataPtr __restrict place, ReadBuffer & rb)
    {
        auto & data_ex = data(place);
        data_ex.deserialize(rb);
    }

    static ALWAYS_INLINE TrackingTime & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<TrackingTime *>(place); }
    static ALWAYS_INLINE const TrackingTime & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const TrackingTime *>(place);
    }

    static void init(AggregateDataPtr __restrict place) { data(place) = TrackingTime{}; }

    static ALWAYS_INLINE bool maxSpanReached(ConstAggregateDataPtr __restrict place, UInt64 span_threshold_ms) noexcept
    {
        return data(place).maxSpanReached(span_threshold_ms);
    }

    ALWAYS_INLINE bool maxSpanReached(UInt64 span_threshold_ms) const noexcept { return max_ts - min_ts >= span_threshold_ms; }

    static void updateTimestamp(AggregateDataPtr __restrict place, UInt64 ts) noexcept { data(place).updateTimestamp(ts); }

    void updateTimestamp(UInt64 ts) noexcept
    {
        min_ts = min_ts > 0 ? std::min(min_ts, ts) : ts;
        max_ts = std::max(max_ts, ts);
    }

    /// In milliseconds
    UInt64 min_ts = 0;
    UInt64 max_ts = 0;
};

}

}
