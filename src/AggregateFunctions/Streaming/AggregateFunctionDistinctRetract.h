#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Streaming/CountedValueMap.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace Streaming
{

/// uint32 max value
constexpr uint32_t INTERNAL_MAP_SIZE = 0xFFFFFFFF;

struct AggregateFunctionDistinctRetractGenericData
{
    /// proton: starts.
    /// When creating, the hash table must be small.
    using Map = CountedValueMap<StringRef, false>; /// map<key(without delta_col), uint32>
    using Self = AggregateFunctionDistinctRetractGenericData;
    Map map;
    std::vector<std::pair<std::string, int8_t>> extra_data_since_last_finalize; /// first element is key, second one is delta_col
    bool use_extra_data = false;  /// Optimized, only streaming global aggregation query need to use extra data after first finalization.
    /// proton: ends.

    AggregateFunctionDistinctRetractGenericData() : map(INTERNAL_MAP_SIZE) { }

    void merge(const Self & rhs, Arena *)
    {
        /// proton: starts.
        if (rhs.use_extra_data)
        {
            for (const auto & [key, delta_col] : rhs.extra_data_since_last_finalize)
            {
                bool inserted = map.insert(key);
                if (use_extra_data && inserted)
                    extra_data_since_last_finalize.emplace_back(key, delta_col);
            }
        }
        else if (use_extra_data)
        {
            for (const auto & [key, count] : rhs.map)
            {
                bool inserted = map.insert(key, count);
                if (inserted)
                    extra_data_since_last_finalize.emplace_back(key.toString(), +1);
            }
        }
        else
        {
            map.merge(rhs.map);
        }
        /// proton: ends.
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(map.size(), buf);
        for (const auto & [key, count] : map)
        {
            writeStringBinary(key, buf);
            writeVarUInt(count, buf);
        }

        writeVectorBinary(extra_data_since_last_finalize, buf);
        writeBoolText(use_extra_data, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        map.clear();

        size_t map_size;
        readVarUInt(map_size, buf);

        uint32_t count;
        for (size_t i = 0; i < map_size; ++i)
        {
            StringRef ref = readStringBinaryInto(*arena, buf);
            readVarUInt(count, buf);
            map.insert(ref, count);
            arena->rollback(ref.size);
        }

        readVectorBinary(extra_data_since_last_finalize, buf);
        readBoolText(use_extra_data, buf);
    }
};

struct AggregateFunctionDistinctRetractMultipleGenericData : public AggregateFunctionDistinctRetractGenericData
{
    void add(StringRef key)
    {
        /// proton: starts.
        auto iter = map.emplace(key);
        bool is_new_inserted_key = (iter != map.end() && iter->second == 1);
        if (use_extra_data && is_new_inserted_key)
            extra_data_since_last_finalize.emplace_back(key.toString(), +1); /// insert a copy versioned key
        /// proton: ends.
    }

    void negate(StringRef key)
    {
        /// proton: starts.
        [[maybe_unused]] bool erase_success = map.erase(key);
        assert(erase_success);
        if (use_extra_data && !map.contains(key))
            extra_data_since_last_finalize.emplace_back(key.toString(), -1);
        /// proton: ends.
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        const size_t argument_size = argument_types.size();
        MutableColumns argument_columns(argument_size);
        for (size_t i = 0; i < argument_size; ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        /// proton: starts.
        if (use_extra_data)
        {
            for (const auto & [key, delta_col] : extra_data_since_last_finalize)
            {
                /// serialize key
                const char * begin = key.c_str();
                for (size_t i = 0; i < argument_size - 1; ++i)
                    begin = argument_columns[i]->deserializeAndInsertFromArena(begin);

                /// insert delta_col
                argument_columns.back()->insert(delta_col);
            }
        }
        else
        {
            for (const auto & [key, _] : map)
            {
                const char * begin = key.data;
                for (size_t i = 0; i < argument_size - 1; ++i)
                    begin = argument_columns[i]->deserializeAndInsertFromArena(begin);

                argument_columns.back()->insert(Int8(1));
            }
        }
        /// proton: ends.

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding _distinct_retract suffix to aggregate function for changelog query
**/
template <typename Data>
class AggregateFunctionDistinctRetract : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctRetract<Data>>
{
protected:
    AggregateFunctionPtr nested_func;
    size_t prefix_size;
    size_t arguments_num;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinctRetract(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctRetract>(arguments, params_)
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        /// We do not serialize the `delta_col` because it is meaningless; calling the `add()` function with only +1 is sufficient.
        for (size_t i = 0; i < arguments_num - 1; ++i)
        {
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        this->data(place).add(value);
        /// Rollback the operation since the arena in this function serves as a data serialization buffer.
        arena->rollback(value.size);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < arguments_num - 1; ++i)
        {
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        this->data(place).negate(value);

        arena->rollback(value.size);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
        nested_func->merge(getNestedPlace(place), getNestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
        nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
        nested_func->deserialize(getNestedPlace(place), buf, std::nullopt /* version */, arena);
    }

    template <bool MergeResult>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto arguments = this->data(place).getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        /// the last position reserved for `delta` col, and one col for the data input.
        assert(arguments.size() >= 2);

        nested_func->addBatchSinglePlace(
            0,
            arguments[0]->size(),
            getNestedPlace(place),
            arguments_raw.data(),
            arena,
            -1 /* if_argument_pos */,
            *(arguments_raw.end() - 1) /* delta_col */);
        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);

        /// proton: starts. Next finalization will use extra data, used buf streaming global aggregation query.
        this->data(place).use_extra_data = true;
        this->data(place).extra_data_since_last_finalize.clear();
        /// proton: ends.
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroy(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "_distinct_retract";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
}
