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
SERDE struct TrackingUpdates
{
    static ALWAYS_INLINE TrackingUpdates & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<TrackingUpdates *>(place); }
    static ALWAYS_INLINE const TrackingUpdates & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const TrackingUpdates *>(place); }

    static ALWAYS_INLINE bool empty(ConstAggregateDataPtr __restrict place) { return data(place).updates == 0; }
    static ALWAYS_INLINE bool updated(ConstAggregateDataPtr __restrict place) { return data(place).updated_since_last_finalization; }
    static ALWAYS_INLINE void setUpdated(AggregateDataPtr __restrict place) { data(place).updated_since_last_finalization = true; }
    static ALWAYS_INLINE void resetUpdated(AggregateDataPtr __restrict place) { data(place).updated_since_last_finalization = false; }

    static void addBatch(size_t row_begin, size_t row_end, AggregateDataPtr * places, const IColumn * delta_col)
    {
        if (delta_col == nullptr)
        {
            for (size_t i = row_begin; i < row_end; ++i)
                if (places[i])
                    data(places[i]).add();
        }
        else
        {
            const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (places[i])
                {
                    if (delta_flags[i] >= 0)
                        data(places[i]).add();
                    else
                        data(places[i]).negate();
                }
            }
        }
    }

    static void addBatchSinglePlace(size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn * delta_col)
    {
        if (!place)
            return;

        auto & data_ex = data(place);
        if (delta_col == nullptr)
            data_ex.updates += row_end - row_begin;
        else
        {
            const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
            data_ex.updates = std::accumulate(delta_flags.begin(), delta_flags.end(), data_ex.updates);
        }

        data_ex.updated_since_last_finalization = true;
    }

    static void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & wb)
    {
        const auto & data_ex = data(place);
        writeIntBinary(data_ex.updates, wb);
        writeBinary(data_ex.updated_since_last_finalization, wb);
    }

    static void deserialize(AggregateDataPtr __restrict place, ReadBuffer & rb)
    {
        auto & data_ex = data(place);
        readIntBinary(data_ex.updates, rb);
        readBinary(data_ex.updated_since_last_finalization, rb);
    }

    ALWAYS_INLINE void add()
    {
        ++updates;
        updated_since_last_finalization = true;
    }

    ALWAYS_INLINE void negate()
    {
        --updates;
        updated_since_last_finalization = true;
    }

    /// Used to track if the target to be tracked has zero sum changes
    UInt64 updates = 0;

    /// Used to track if the target group tracked has updates since last finalization 
    bool updated_since_last_finalization = true;
};

enum class TrackingUpdatesType : uint8_t
{
    None = 0,
    Updates = 1,
};

}
}
