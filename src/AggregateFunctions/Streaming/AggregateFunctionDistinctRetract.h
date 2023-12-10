#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <Common/HashTable/HashMap.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{
namespace Streaming
{

template <typename T>
struct AggregateFunctionDistinctRetractSingleNumericData
{
    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<T, UInt64, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctRetractSingleNumericData<T>;
    Map map;

    /// proton: starts. Resolve multiple finalizations problem for streaming global aggregation query
    absl::flat_hash_map<T, uint64_t, absl::Hash<T>> extra_data_since_last_finalize;
    bool use_extra_data = false;  /// Optimized, only streaming global aggregation query need to use extra data after first finalization.
    /// proton: ends.

    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        /// proton: starts.
        map[vec[row_num]]++;
        if (use_extra_data)
            extra_data_since_last_finalize[vec[row_num]]++;
        /// proton: ends.
    }

    void negate(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        /// proton: starts.

        const auto & inserted_data = vec[row_num];

        map[inserted_data]--;
        if (map[inserted_data] == 0)
            map.erase(inserted_data);


        if (use_extra_data)
        {
            extra_data_since_last_finalize[inserted_data]--;
            if (extra_data_since_last_finalize[inserted_data] == 0)
                extra_data_since_last_finalize.erase(inserted_data);
        }
        /// proton: ends.
    }

    void merge(const Self & rhs, Arena *)
    {
        /// proton: starts.
        if (rhs.use_extra_data)
        {
            for (const auto & pair : rhs.extra_data_since_last_finalize)
            {
                map[pair.first] += pair.second;
                if (use_extra_data)
                    extra_data_since_last_finalize[pair.first] += pair.second;
            }
        }
        else if (use_extra_data)
        {
            for (const auto & pair : rhs.map)
            {
                map[pair.getKey()] += pair.getMapped();

                extra_data_since_last_finalize[pair.getKey()] += pair.getMapped();
            }
        }
        else
        {
            for (const auto & pair : rhs.map)
                map[pair.getKey()] += pair.getMapped();
        }
        /// proton: ends.
    }

    void serialize(WriteBuffer & buf) const
    {
        /// proton: starts.
        /// serialize historical data:

        writeVarUInt(map.size(), buf);
        for (const auto & elem : map)
        {
            writeBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }

        /// serialize increment data:

        writeVarUInt(extra_data_since_last_finalize.size(), buf);
        for (const auto & pair : extra_data_since_last_finalize)
        {
            writeBinary(pair.first, buf);
            writeVarUInt(pair.second, buf);
        }
        writeBoolText(use_extra_data, buf);
        /// proton: ends.
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        /// proton: starts.
        size_t his_data_size = 0;
        readVarUInt(his_data_size, buf);
        map.reserve(his_data_size);

        T key;
        uint64_t value;
        for (size_t i = 0; i < his_data_size; ++i)
        {
            readBinary(key, buf);
            readVarUInt(value, buf);
            map[key] = value;
        }

        size_t stream_data_size = 0;
        readVarUInt(stream_data_size, buf);
        extra_data_since_last_finalize.reserve(stream_data_size);

        auto hint = extra_data_since_last_finalize.end();  
        for (size_t i = 0; i < stream_data_size; ++i)
        {
            readBinary(key, buf);
            readVarUInt(value, buf);
            hint = extra_data_since_last_finalize.emplace_hint(hint, key, value); 
        }
        readBoolText(use_extra_data, buf);
        /// proton: ends.
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        /// proton: starts.
        if (use_extra_data)
        {
            for (const auto & pair : extra_data_since_last_finalize)
                argument_columns[0]->insert(pair.first);
        }
        else
        {
            for (const auto & elem : map)
                argument_columns[0]->insert(elem.getKey());
        }
        /// proton: ends.

        return argument_columns;
    }
};

struct AggregateFunctionDistinctRetractGenericData
{
    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<StringRef, UInt64, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctRetractGenericData;
    Map map;
    /// proton: starts. Resolve multiple finalizations problem for streaming global aggregation query
    absl::flat_hash_map<StringRef, UInt64, StringRefHash> extra_data_since_last_finalize;
    bool use_extra_data = false;  /// Optimized, only streaming global aggregation query need to use extra data after first finalization.
    /// proton: ends.

    void merge(const Self & rhs, Arena * arena)
    {
        Map::LookupResult it;
        bool inserted;
        for (const auto & elem : rhs.map)
        /// proton: starts.
        {
            map.emplace(ArenaKeyHolder{elem.getKey(), *arena}, it, inserted);
            if (inserted)
                new (&it->getMapped()) UInt64(elem.getMapped());
            else
                new (&it->getMapped()) UInt64(elem.getMapped() + it->getMapped());

            if (use_extra_data)
            {
                assert(it);
                auto [it_new, inserted_new] = extra_data_since_last_finalize.try_emplace(it->getKey(), elem.getMapped());
                if (!inserted_new)
                    it_new->second += elem.getMapped();
            }
        }
        /// proton: ends.
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(map.size(), buf);
        for (const auto & elem : map)
        {
            writeStringBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }

        /// proton: starts.
        writeVarUInt(extra_data_since_last_finalize.size(), buf);
        for (const auto & pair : extra_data_since_last_finalize)
        {
            writeStringBinary(pair.first, buf);
            writeVarUInt(pair.second, buf);
        }

        writeBoolText(use_extra_data, buf);
        /// proton: ends.
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        map.reserve(size);

        StringRef key_data;
        uint64_t value_data;
        for (size_t i = 0; i < size; ++i) 
        {
            key_data = readStringBinaryInto(*arena, buf);
            readVarUInt(value_data, buf);
            map[key_data] = value_data;
        }

        /// proton: starts.
        size_t extra_size;
        readVarUInt(extra_size, buf);
        extra_data_since_last_finalize.reserve(extra_size);

        auto hint = extra_data_since_last_finalize.end();  
        for (size_t i = 0; i < extra_size; ++i)
        {
            key_data = readStringBinaryInto(*arena, buf);
            readVarUInt(value_data, buf);
            hint = extra_data_since_last_finalize.emplace_hint(hint, key_data, value_data); 
        }   

        readBoolText(use_extra_data, buf);
        /// proton: ends.
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctRetractSingleGenericData : public AggregateFunctionDistinctRetractGenericData
{
    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        auto & key_data = keyHolderGetKey(key_holder);
        map[key_data]++;

        /// proton: starts.
        if (use_extra_data )
            extra_data_since_last_finalize[key_data]++;
        /// proton: ends.
    }

    void negate(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        auto & key_data = keyHolderGetKey(key_holder);
        map[key_data]--;
        if (map[key_data] == 0)
            map.erase(key_data);

        /// proton: starts.
        if (use_extra_data)
        {
            extra_data_since_last_finalize[key_data]--;
            if (extra_data_since_last_finalize[key_data] == 0)
                extra_data_since_last_finalize.erase(key_data);
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        /// proton: starts.
        if (use_extra_data)
        {
            for (const auto & data : extra_data_since_last_finalize)
                deserializeAndInsert<is_plain_column>(data.first, *argument_columns[0]);
        }
        else
        {
            for (const auto & elem : map)
                deserializeAndInsert<is_plain_column>(elem.getKey(), *argument_columns[0]);
        }
        /// proton: ends.

        return argument_columns;
    }
};

struct AggregateFunctionDistinctRetractMultipleGenericData : public AggregateFunctionDistinctRetractGenericData
{
    void add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        auto key_holder = SerializedKeyHolder{value, *arena};
        auto & key_data = keyHolderGetKey(key_holder);
        map[key_data]++;

        /// proton: starts.
        if (use_extra_data)
            extra_data_since_last_finalize[key_data]++;
        /// proton: ends.
    }

    void negate(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        auto key_holder = SerializedKeyHolder{value, *arena};
        auto & key_data = keyHolderGetKey(key_holder);
        map[key_data]--;
        if (map[key_data] == 0)
            map.erase(key_data);

        /// proton: starts.
        if (use_extra_data)
        {
            extra_data_since_last_finalize[key_data]--;
            if (extra_data_since_last_finalize[key_data] == 0)
                extra_data_since_last_finalize.erase(key_data);
        }
        /// proton: ends.
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        /// proton: starts.
        if (use_extra_data)
        {
            for (const auto & pair : extra_data_since_last_finalize)
            {
                const char * begin = pair.first.data;
                for (auto & column : argument_columns)
                    begin = column->deserializeAndInsertFromArena(begin);
            }
        }
        else
        {
            for (const auto & elem : map)
            {
                const char * begin = elem.getKey().data;
                for (auto & column : argument_columns)
                    begin = column->deserializeAndInsertFromArena(begin);
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
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).negate(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
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

        nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena, -1 /* if_argument_pos */, *(arguments_raw.end()-1) /* delta_col */);
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
