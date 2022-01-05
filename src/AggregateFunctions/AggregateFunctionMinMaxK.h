#pragma once

#include "IAggregateFunction.h"

#include <Columns/ColumnArray.h>
#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

constexpr size_t TOP_K_MAX_SIZE = 0xFFFFFF;

template <typename T, bool is_min>
struct AggregateFunctionMinMaxKData
{
    using Container = typename std::vector<T, AllocatorWithMemoryTracking<T>>;
    using Compare = std::function<bool(const T &, const T &)>;
    Container values;
    SpaceSavingArena<T> arena;
    Compare compare;
    bool is_sorted = false;

    AggregateFunctionMinMaxKData()
    {
        /// We ingore the Nan or NULL, so
        /// if is minK, `nan_direction_hint=1` are considered larger than all numbers
        /// if is maxK, `nan_direction_hint=-1` are considered less than all numbers
        if constexpr (is_min)
            compare = std::bind(CompareHelper<T>::less, std::placeholders::_1, std::placeholders::_2, /* nan_direction_hint */ 1);
        else
            compare = std::bind(CompareHelper<T>::greater, std::placeholders::_1, std::placeholders::_2, /* nan_direction_hint */ -1);
        std::make_heap(values.begin(), values.end(), compare);
    }

    void add(const T & val, UInt64 k)
    {
        if (is_sorted)
        {
            std::make_heap(values.begin(), values.end(), compare);
            is_sorted = false;
        }

        if (values.size() < k)
        {
            values.push_back(arena.emplace(val));
            std::push_heap(values.begin(), values.end(), compare);
        }
        else if (compare(val, values.front()))
        {
            values.push_back(arena.emplace(val));
            std::push_heap(values.begin(), values.end(), compare);
            std::pop_heap(values.begin(), values.end(), compare);
            arena.free(values.back());
            values.pop_back();
        }
    }

    void reserve(UInt64 k)
    {
        if (unlikely(values.capacity() < k))
            values.reserve(k);
    }

    typename Container::const_iterator begin() const { return values.begin(); }

    typename Container::const_iterator end() const { return values.end(); }

    size_t size() const { return values.size(); }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(values.size(), wb);

        for (const auto & value : values)
            writeBinary(value, wb);

        writeBoolText(is_sorted, wb);
    }

    void read(ReadBuffer & rb)
    {
        size_t size = 0;
        readVarUInt(size, rb);

        assert(size < TOP_K_MAX_SIZE);

        /// We free current values before read.
        for (const auto & value : values)
            arena.free(value);

        values.resize(size);
        for (size_t i = 0; i < size; ++i)
            readBinary(values[i], rb);

        readBoolText(is_sorted, rb);
    }

    void sort()
    {
        if (!is_sorted)
        {
            std::sort_heap(values.begin(), values.end(), compare);
            is_sorted = true;
        }
    }
};


template <typename T, bool is_min>
class AggregateFunctionMinMaxK
    : public IAggregateFunctionDataHelper<AggregateFunctionMinMaxKData<T, is_min>, AggregateFunctionMinMaxK<T, is_min>>
{
protected:
    using State = AggregateFunctionMinMaxKData<T, is_min>;
    UInt64 k;
    UInt64 reserved;

public:
    AggregateFunctionMinMaxK(UInt64 k_, const DataTypePtr & argument_type_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionMinMaxKData<T, is_min>, AggregateFunctionMinMaxK<T, is_min>>(
            {argument_type_}, params)
        , k(k_)
        , reserved(k_ + 1)
    {
    }

    String getName() const override { return is_min ? "minK" : "maxK"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(this->argument_types[0]); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);
        top_k.add(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], k);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        for (const auto & elem : this->data(rhs))
            top_k.add(elem, k);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);
        top_k.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        auto & top_k = this->data(place);
        size_t size = top_k.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        top_k.sort();

        size_t i = 0;
        for (auto it = top_k.begin(); it != top_k.end(); ++it, ++i)
            data_to[old_size + i] = *it;
    }
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns minMaxK() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column, bool is_min>
class AggregateFunctionMinMaxKGeneric : public IAggregateFunctionDataHelper<
                                            AggregateFunctionMinMaxKData<StringRef, is_min>,
                                            AggregateFunctionMinMaxKGeneric<is_plain_column, is_min>>
{
private:
    using State = AggregateFunctionMinMaxKData<StringRef, is_min>;

    UInt64 k;
    UInt64 reserved;
    DataTypePtr & input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionMinMaxKGeneric(UInt64 k_, const DataTypePtr & input_data_type_, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregateFunctionMinMaxKData<StringRef, is_min>,
            AggregateFunctionMinMaxKGeneric<is_plain_column, is_min>>({input_data_type_}, params)
        , k(k_)
        , reserved(k_ + 1)
        , input_data_type(this->argument_types[0])
    {
    }

    String getName() const override { return is_min ? "minK" : "maxK"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        // Specialized here because there's no deserialiser for StringRef
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            top_k.add(ref, k);
            arena->rollback(ref.size);
        }

        readBoolText(top_k.is_sorted, buf);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        if constexpr (is_plain_column)
        {
            top_k.add(columns[0]->getDataAt(row_num), k);
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            top_k.add(str_serialized, k);
            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        for (const auto & elem : this->data(rhs))
            top_k.add(elem, k);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & top_k = this->data(place);
        offsets_to.push_back(offsets_to.back() + top_k.size());

        top_k.sort();

        for (auto & elem : top_k)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.data, elem.size);
            else
                data_to.deserializeAndInsertFromArena(elem.data);
        }
    }
};

}
