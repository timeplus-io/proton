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

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF

/// proton: starts.
#include <absl/container/flat_hash_set.h>
/// proton: ends.

namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;

    Set value;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, typename LimitNumElems>
class AggregateFunctionGroupUniqArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T, LimitNumElems>>
{
    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

private:
    using State = AggregateFunctionGroupUniqArrayData<T>;

public:
    AggregateFunctionGroupUniqArray(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
          AggregateFunctionGroupUniqArray<T, LimitNumElems>>({argument_type}, parameters_, std::make_shared<DataTypeArray>(argument_type)),
          max_elems(max_elems_) {}

    AggregateFunctionGroupUniqArray(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
          AggregateFunctionGroupUniqArray<T, LimitNumElems>>({argument_type}, parameters_, result_type_),
          max_elems(max_elems_) {}

    String getName() const override { return "group_uniq_array"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if (limit_num_elems && this->data(place).value.size() >= max_elems)
            return;
        this->data(place).value.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        if (!limit_num_elems)
            this->data(place).value.merge(this->data(rhs).value);
        else
        {
            auto & cur_set = this->data(place).value;
            auto & rhs_set = this->data(rhs).value;

            for (auto & rhs_elem : rhs_set)
            {
                if (cur_set.size() >= max_elems)
                    return;
                cur_set.insert(rhs_elem.getValue());
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        size_t size = set.size();
        writeVarUInt(size, buf);
        for (const auto & elem : set)
            writeIntBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).value.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        size_t size = set.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash,
        INITIAL_SIZE_DEGREE>;

    Set value;
};

template <bool is_plain_column>
static void deserializeAndInsertImpl(StringRef str, IColumn & data_to);

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns group_uniq_array() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename LimitNumElems = std::false_type>
class AggregateFunctionGroupUniqArrayGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData,
        AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupUniqArrayGeneric(const DataTypePtr & input_data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData, AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems>>({input_data_type_}, parameters_, std::make_shared<DataTypeArray>(input_data_type_))
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_) {}

    String getName() const override { return "group_uniq_array"; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
        {
            writeStringBinary(elem.getValue(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        size_t size;
        readVarUInt(size, buf);
        //TODO: set.reserve(size);

        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (limit_num_elems && set.size() >= max_elems)
            return;

        bool inserted;
        typename State::Set::LookupResult it;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        bool inserted;
        typename State::Set::LookupResult it;
        for (auto & rhs_elem : rhs_set)
        {
            if (limit_num_elems && cur_set.size() >= max_elems)
                return;

            // We have to copy the keys to our arena.
            cur_set.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), data_to);
    }
};

/// proton: starts.
struct AggregateFunctionGroupUniqArrayGenericDataWithoutArena
{
    absl::flat_hash_set<String> value;
};

template <bool is_plain_column = false, typename LimitNumElems = std::false_type>
class AggregateFunctionGroupUniqArrayGenericWithoutArena
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericDataWithoutArena,
        AggregateFunctionGroupUniqArrayGenericWithoutArena<is_plain_column, LimitNumElems>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

    using Self = AggregateFunctionGroupUniqArrayGenericWithoutArena<is_plain_column, LimitNumElems>;
    using State = AggregateFunctionGroupUniqArrayGenericDataWithoutArena;

public:
    AggregateFunctionGroupUniqArrayGenericWithoutArena(const DataTypePtr & input_data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<State, Self>({input_data_type_}, parameters_, std::make_shared<DataTypeArray>(input_data_type_))
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_) {}

    String getName() const override { return "group_uniq_array"; }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
            writeStringBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & set = this->data(place).value;
        size_t size;
        readVarUInt(size, buf);

        set.reserve(size);
        String tmp_value;
        for (size_t i = 0; i < size; ++i)
        {
            readStringBinary(tmp_value, buf);
            set.emplace(std::move(tmp_value));
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (limit_num_elems && set.size() >= max_elems)
            return;

        if constexpr (is_plain_column)
        {
            set.emplace(columns[0]->getDataAt(row_num));
        }
        else
        {
            Arena tmp_arena(256); /// used as a temporary buffer if \is_plain_column=false
            auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, tmp_arena);
            set.emplace(key_holder.key);
            keyHolderDiscardKey(key_holder);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;
        cur_set.insert(rhs_set.begin(), rhs_set.end());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (const auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem, data_to);
    }
};
/// proton:ends.

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

}
