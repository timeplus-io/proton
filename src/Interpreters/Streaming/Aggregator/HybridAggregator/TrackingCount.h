#pragma once

#include <numeric>

namespace DB::Streaming
{

SERDE struct TrackingCount
{
    static ALWAYS_INLINE TrackingCount & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<TrackingCount *>(place); }
    static ALWAYS_INLINE const TrackingCount & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const TrackingCount *>(place);
    }
    static ALWAYS_INLINE void init(AggregateDataPtr __restrict place) { new (place) TrackingCount(); }
    static ALWAYS_INLINE void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs)
    {
        data(place).merge(data(rhs));
    }
    static ALWAYS_INLINE void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & wb) { data(place).serialize(wb); }
    static ALWAYS_INLINE void deserialize(AggregateDataPtr __restrict place, ReadBuffer & rb) { data(place).deserialize(rb); }

    static ALWAYS_INLINE bool empty(ConstAggregateDataPtr __restrict place) { return data(place).count == 0; }

    static void addBatch(size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t offset, const IColumn * delta_col)
    {
        if (delta_col == nullptr)
        {
            for (size_t i = row_begin; i < row_end; ++i)
                if (places[i])
                    ++data(places[i] + offset).count;
        }
        else
        {
            const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (places[i])
                {
                    if (delta_flags[i] >= 0)
                        ++data(places[i] + offset).count;
                    else
                        --data(places[i] + offset).count;
                }
            }
        }
    }

    static void addBatchSinglePlace(size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn * delta_col)
    {
        if (!place)
            return;

        auto & state = data(place);
        if (delta_col == nullptr)
        {
            state.count += row_end - row_begin;
        }
        else
        {
            const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
            state.count = std::accumulate(delta_flags.begin(), delta_flags.end(), state.count);
        }
    }

    void merge(const TrackingCount & rhs) { count += rhs.count; }
    void serialize(WriteBuffer & wb) const { DB::writeVarUInt(count, wb); }
    void deserialize(ReadBuffer & rb) { DB::readVarUInt(count, rb); }

    uint64_t count = 0;
};

}
