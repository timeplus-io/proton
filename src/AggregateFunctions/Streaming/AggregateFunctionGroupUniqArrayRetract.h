#pragma once

#include <cassert>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <AggregateFunctions/Streaming/CountedValueMap.h>



namespace DB
{
struct Settings;

namespace Streaming
{
template <typename T>
struct AggregateFunctionGroupUniqArrayRetractData
{
    using Map = CountedValueMap<T, false>;
    Map map;
};

/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, typename LimitNumElems>
class AggregateFunctionGroupUniqArrayRetract
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayRetractData<T>, AggregateFunctionGroupUniqArrayRetract<T, LimitNumElems>>
{
    static constexpr bool limit_num_elems = LimitNumElems::value;
    Int64 max_elems;

private:
    using State = AggregateFunctionGroupUniqArrayRetractData<T>;

public:
    AggregateFunctionGroupUniqArrayRetract(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayRetractData<T>,
          AggregateFunctionGroupUniqArrayRetract<T, LimitNumElems>>({argument_type}, parameters_),
          max_elems(max_elems_) {}

    String getName() const override { return "group_uniq_array_retract"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(this->argument_types[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if (limit_num_elems && this->data(place).map.size() >= max_elems)
            return;
        
        this->data(place).map.emplace(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        bool erase_success = this->data(place).map.erase(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
        assert(erase_success);
    }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & cur_map = this->data(place).map;
        auto & rhs_map = this->data(rhs).map;

        if (limit_num_elems && cur_map.size() >= max_elems)
            return;
        
        cur_map.merge(rhs_map);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).map;
        size_t size = map.size();
        writeVarUInt(size, buf);
        for (const auto & [key, count] : map)
        {
            writeIntBinary(key, buf);
            writeIntBinary(count, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & map = this->data(place).map;
        size_t map_size = 0;
        readVarUInt(map_size, buf);
        for (size_t i = 0; i < map_size; i++)
        {
            T key;
            readIntBinary(key, buf);
            uint32_t count = 0;
            readIntBinary(count, buf);
            map.insert(key, count);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & map = this->data(place).map;
        size_t size = map.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        // for (auto it = set.begin(); it != set.end(); ++it, ++i)
        //     data_to[old_size + i] = it->getValue();
        for (const auto & [key, _] : map)
        {
            data_to[old_size + i] = key;
            i++;
        }
    }
};

/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupUniqArrayRetractGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Map = CountedValueMap<StringRef, false>;


    Map map;
};


/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns group_uniq_array() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename LimitNumElems = std::false_type>
class AggregateFunctionGroupUniqArrayRetractGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayRetractGenericData,
        AggregateFunctionGroupUniqArrayRetractGeneric<is_plain_column, LimitNumElems>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = LimitNumElems::value;
    Int64 max_elems;

    using State = AggregateFunctionGroupUniqArrayRetractGenericData;

public:
    AggregateFunctionGroupUniqArrayRetractGeneric(const DataTypePtr & input_data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayRetractGenericData, AggregateFunctionGroupUniqArrayRetractGeneric<is_plain_column, LimitNumElems>>({input_data_type_}, parameters_)
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_) {}

    String getName() const override { return "group_uniq_array_retract"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(input_data_type);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).map;
        writeVarUInt(map.size(), buf);
        for (const auto & [key, count] : map)
        {
            writeStringBinary(key, buf);
            writeIntBinary(count, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & map = this->data(place).map;
        size_t size;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            StringRef ref = readStringBinaryInto(*arena, buf);
            uint32_t count = 0;
            readVarUInt(count, buf);
            map.insert(ref, count);
            arena->rollback(ref.size);
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & map = this->data(place).map;
        if (limit_num_elems && map.size() >= max_elems)
            return;

        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        map.emplace(key_holder.key);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const char * begin = nullptr;
        auto ref = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
        this->data(place).map.erase(ref);
        arena->rollback(ref.size);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_map = this->data(place).map;
        auto & rhs_map = this->data(rhs).map;

        if (limit_num_elems && cur_map.size() >= max_elems)
            return;
        
        cur_map.merge(rhs_map);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & map = this->data(place).map;
        offsets_to.push_back(offsets_to.back() + map.size());

        for (const auto & [key, _] : map)
            deserializeAndInsert<is_plain_column>(key, data_to);
    }
};
}
}