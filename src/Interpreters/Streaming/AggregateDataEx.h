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
SERDE struct UpdatedDataEx
{
    static ALWAYS_INLINE UpdatedDataEx & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<UpdatedDataEx *>(place); }
    static ALWAYS_INLINE const UpdatedDataEx & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const UpdatedDataEx *>(place); }

    static ALWAYS_INLINE bool isEmpty(ConstAggregateDataPtr __restrict place) { return data(place).final_count == 0; }
    static ALWAYS_INLINE bool isUpdated(ConstAggregateDataPtr __restrict place) { return data(place).updated_since_last_finalization; }
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

        auto & metadata = data(place);
        if (delta_col == nullptr)
            metadata.final_count += row_end - row_begin;
        else
        {
            const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
            metadata.final_count = std::accumulate(delta_flags.begin(), delta_flags.end(), metadata.final_count);
        }

        metadata.updated_since_last_finalization = true;
    }

    static void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & wb)
    {
        const auto & data_ex = data(place);
        writeIntBinary(data_ex.final_count, wb);
        writeBoolText(data_ex.updated_since_last_finalization, wb);
    }

    static void deserialize(AggregateDataPtr __restrict place, ReadBuffer & rb)
    {
        auto & data_ex = data(place);
        readIntBinary(data_ex.final_count, rb);
        readBoolText(data_ex.updated_since_last_finalization, rb);
    }

    ALWAYS_INLINE void add()
    {
        ++final_count;
        updated_since_last_finalization = true;
    }

    ALWAYS_INLINE void negate()
    {
        --final_count;
        updated_since_last_finalization = true;
    }

    /// Used for tracking the group is empty or not
    UInt32 final_count = 0;

    /// Used for tracking the group is updated or not
    bool updated_since_last_finalization = true;
};

SERDE struct RetractedDataEx : UpdatedDataEx
{
    static ALWAYS_INLINE AggregateDataPtr & getRetracted(AggregateDataPtr & place) { return reinterpret_cast<RetractedDataEx *>(place)->retracted_data; }
    static ALWAYS_INLINE bool hasRetracted(ConstAggregateDataPtr __restrict place) { return reinterpret_cast<const RetractedDataEx *>(place)->retracted_data; }

    template <bool use_retracted_data>
    static ALWAYS_INLINE AggregateDataPtr & getData(AggregateDataPtr & place)
    {
        if constexpr (use_retracted_data)
            return getRetracted(place);
        else
            return place;
    }

    /// Used for tracking group changes
    AggregateDataPtr retracted_data = nullptr;
};

enum class ExpandedDataType : uint8_t
{
    None = 0,
    Updated = 1, /// Allow tracking group is empty or updated
    UpdatedWithRetracted = 2, /// Allow tracking group is empty or updated and changes
};

}
}
