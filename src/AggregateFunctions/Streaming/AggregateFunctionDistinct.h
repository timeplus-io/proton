#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/HashTable/HashSet.h>
#include <Common/assert_cast.h>
#include <Common/serde.h>

namespace DB
{
namespace Streaming
{

/*  Notes on distinct function:

    For example: Using sum_distinct function in a two shards situation. 
                 During the first insertion, we may insert all the data to one shard, or we may insert them into different shards.

    - We use the first scenerio as an example, during which don't need to create a new temp state for merge, sum will
      directly add using extra data and the data will be cleared after finalization. (This is similar to single shard situation.)

      We won't merge the state but directly use the data. Which will cause the extra data not been able to recorded
      after we use another shard, for the extra data have been cleared after first finalization.
                  ______________                       _______________ 
                 |  <state 1>   |                     |   <state 1>   |
Insert 30,40     |  set:30 40   |       finalize      |   set:30 40   |
                 | extra:30 40  |     ————————————>   |  extra:none   |  *extra data will be cleared after every merge
                 | nested_sum:0 |                     | nested_sum:70 | 
                 |______________|                     |_______________| 

    - After data been inserted into the other shard, causing the merging action been activatied as shown in the graphic.
      The aggregate state we using here is temporary, therefore we need to find first aggregate state and store sum in it. 
      Here we use the shard after the finalization in single situation.

      *sum(temp state) = sum(data1) + sum(data2) + extra
                     ______________   ______________                  _______________   ______________ 
                    |  <state 1>   | |  <state 2>   |                |   <state 1>   | |  <state 2>   |
                    |  set:30 40   | |   set:40 50  |                |  set:30 40    | |   set:40 50  |
                    |  extra:none  | |  extra:40 50 |                |  extra:none   | |  extra:none  |
                    | nested_sum:70| | nested_sum:0 |                | nested_sum:120| | nested_sum:0 |
                    |______________| |______________|                |_______________| |______________| 
Insert 40,50               |                |                                |                 |
into shard2                |                |  *step2: Finalize and          |                 |
                           |  *step1:Merge  |   add extra data back to       |                 |
                     ______▼________        |   nested_sum (70+50=120)_______▼________         |
                    | <temp state>  |       |     ——————————————>    |  <temp state>  |        |
                    |  set:30 40 50 | ◀------                        |  set:30 40 50  | ◀-------
                    |  extra:50     |                                |   extra:none   |
                    | nested_sum:70 |                                | nested_sum:120 |  
                    |_______________|                                |________________|
    
                     _______________   ______________                        _______________   ______________ 
                    |   <state 1>   | |  <state 2>   |                      |   <state 1>   | |  <state 2>   |                      
                    |  set:30 40 60 | |   set:40 50  | *extra data will     |  set:30 40 60 | |   set:40 50  |
                    |  extra:60     | |  extra:none  |  be cleared after    |  extra:none   | |  extra:none  |
                    | nested_sum:120| | nested_sum:0 |  every merge         | nested_sum:180| | nested_sum:0 |
                    |_______________| |______________|                      |_______________| |______________|
                            |                 |               *step2                |                |
Insert 60                   |                 |         ——————————————————>         |                |
into shard1                 |     *step1      |                                     |                |
                     _______▼________         |                              _______▼________        |
                    |  <temp state>  |        |                             |  <temp state>  |       |
                    | set:30 40 50 60| ◀-------                             | set:30 40 50 60| ◀------ 
                    |   extra:60     |                                      |   extra:none   |       
                    | nested_sum:120 | *sum won't accumulate without        | nested_sum:180 | 
                    |________________|  addback in step 2 because sum in    |________________|
                                        temp state will be cleared everytime


    step1: Check if there's extra data that already existed in set and delete it, then Merge the set to prevent shards afterwards inserting 
           data exists in set.
    
    step2: If the current sum is 70, what we stored in nested_sum of temporary state will be 70, but it will be deleted after finalization. 
           So we find the information of the first aggregate state stored in merged_places and put 70 to its sum after the merging
           using nested_func->addBatchSinglePlace, thus the sum will accumulate.

*/
template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;
    Set set;

    /// Resolve multiple finalizations problem for streaming global aggreagtion query
    /// Optimized, put the new coming data that the set does not have into extra_data_since_last_finalize.
    std::vector<T> extra_data_since_last_finalize;

    NO_SERDE std::vector<uintptr_t> merged_places;

    /// not used, but is kept for backward compatibility
    bool use_extra_data = false;

    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        auto [_, inserted] = set.insert(vec[row_num]);
        if (inserted)
            extra_data_since_last_finalize.emplace_back(vec[row_num]);
    }

    void merge(const Self & rhs, Arena *)
    {
        /// Deduplicate owned extra data based on rhs
        for (auto it = extra_data_since_last_finalize.begin(); it != extra_data_since_last_finalize.end();)
        {
            if (rhs.set.find(*it) != rhs.set.end())
                it = extra_data_since_last_finalize.erase(it);
            else
                ++it;
        }

        /// Merge and deduplicate rhs' extra data
        for (const auto & data : rhs.extra_data_since_last_finalize)
        {
            auto [_, inserted] = set.insert(data);
            if (inserted)
                extra_data_since_last_finalize.emplace_back(data);
        }

        set.merge(rhs.set);

        uintptr_t merged_place = reinterpret_cast<uintptr_t>(&rhs);
        auto find_place = std::find(merged_places.begin(), merged_places.end(), merged_place);
        if (find_place == merged_places.end())
            merged_places.emplace_back(merged_place);
    }

    void serialize(WriteBuffer & buf) const
    {
        set.write(buf);
        writeVectorBinary(extra_data_since_last_finalize, buf);
        writeBoolText(use_extra_data, buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        set.read(buf);
        readVectorBinary(extra_data_since_last_finalize, buf);
        readBoolText(use_extra_data, buf);
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        for (const auto & data : extra_data_since_last_finalize)
            argument_columns[0]->insert(data);

        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;
    Set set;
    /// Resolve multiple finalizations problem for streaming global aggreagtion query
    /// Optimized, put the new coming data that the set does not have into extra_data_since_last_finalize.
    std::vector<StringRef> extra_data_since_last_finalize;

    NO_SERDE std::vector<uintptr_t> merged_places;

    bool use_extra_data = false;

    void merge(const Self & rhs, Arena * arena)
    {
        Set::LookupResult it;
        bool inserted;
        /// Deduplicate owned extra data based on rhs
        for (auto next = extra_data_since_last_finalize.begin(); next != extra_data_since_last_finalize.end();)
        {
            if (rhs.set.find(*next) != rhs.set.end())
                next = extra_data_since_last_finalize.erase(next);
            else
                ++next;
        }

        /// Merge and deduplicate rhs' extra data
        for (const auto & data : rhs.extra_data_since_last_finalize)
        {
            set.emplace(ArenaKeyHolder{data, *arena}, it, inserted);
            if (inserted)
            {
                assert(it);
                extra_data_since_last_finalize.emplace_back(it->getValue());
            }
        }

        set.merge(rhs.set);

        uintptr_t merged_place = reinterpret_cast<uintptr_t>(&rhs);
        auto find_place = std::find(merged_places.begin(), merged_places.end(), merged_place);
        if (find_place == merged_places.end())
            merged_places.emplace_back(merged_place);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(set.size(), buf);
        for (const auto & elem : set)
            writeStringBinary(elem.getValue(), buf);

        writeVarUInt(extra_data_since_last_finalize.size(), buf);
        for (const auto & data : extra_data_since_last_finalize)
            writeStringBinary(data, buf);

        writeBoolText(use_extra_data, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));

        size_t extra_size;
        readVarUInt(extra_size, buf);
        extra_data_since_last_finalize.resize(extra_size);
        for (size_t i = 0; i < extra_size; ++i)
            extra_data_since_last_finalize[i] = readStringBinaryInto(*arena, buf);

        readBoolText(use_extra_data, buf);
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData
{
    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);

        if (inserted)
        {
            assert(it);
            extra_data_since_last_finalize.emplace_back(it->getValue());
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        for (const auto & data : extra_data_since_last_finalize)
            deserializeAndInsert<is_plain_column>(data, *argument_columns[0]);

        return argument_columns;
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData
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

        Set::LookupResult it;
        bool inserted;
        auto key_holder = SerializedKeyHolder{value, *arena};
        set.emplace(key_holder, it, inserted);

        if (inserted)
        {
            assert(it);
            extra_data_since_last_finalize.emplace_back(it->getValue());
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto & data : extra_data_since_last_finalize)
        {
            const char * begin = data.data;
            for (auto & column : argument_columns)
                begin = column->deserializeAndInsertFromArena(begin);
        }

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding _distinct_streaming suffix to aggregate function for streaming query
**/
template <typename Data>
class AggregateFunctionDistinct : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>>
{
protected:
    AggregateFunctionPtr nested_func;
    size_t prefix_size;
    size_t arguments_num;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(arguments, params_)
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
        auto & data = this->data(place);
        auto arguments = data.getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        assert(!arguments.empty());
        /// Accumulation for current data block
        nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);

        /// Clear all the extra data in related blocks
        for (auto & merged_place : data.merged_places)
            this->data(reinterpret_cast<AggregateDataPtr>(merged_place)).extra_data_since_last_finalize.clear();

        if (!data.merged_places.empty())
            nested_func->addBatchSinglePlace(
                0,
                arguments[0]->size(),
                getNestedPlace(reinterpret_cast<AggregateDataPtr>(data.merged_places[0])),
                arguments_raw.data(),
                arena);

        data.extra_data_since_last_finalize.clear();
        data.merged_places.clear();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    size_t sizeOfData() const override { return prefix_size + nested_func->sizeOfData(); }

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

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor(); }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override { return nested_func->getName() + "_distinct_streaming"; }

    DataTypePtr getReturnType() const override { return nested_func->getReturnType(); }

    bool allocatesMemoryInArena() const override { return true; }

    bool isState() const override { return nested_func->isState(); }

    bool isVersioned() const override { return nested_func->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_func->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_func->getDefaultVersion(); }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
}
