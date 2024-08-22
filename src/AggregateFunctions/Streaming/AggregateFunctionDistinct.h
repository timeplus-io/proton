#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{
namespace Streaming
{
enum class IsBlank
{
    /// A block that contains new data, whose data will not be deleted.
    DataBlock,
    /// A block that appears in special occasion, as the first insertion didn't call merge, the extra_data of this block will be deleted.
    /// So we'll need to mark the block and merge all the data in set to check if there's an overlap
    LessExtraDataBlock,
    /// A block that is newly created for merge, its extra_data must have been cleared.
    BlankBlock,
};
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

    std::vector<uintptr_t> related_places;

    /// not used
    bool use_extra_data = false;
    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        auto [_, inserted] = set.insert(vec[row_num]);
        if (inserted)
        {
            extra_data_since_last_finalize.emplace_back(vec[row_num]);
        }
    }

    void merge(const Self & rhs, Arena *, ConstAggregateDataPtr __restrict place)
    {
        for (const auto & data : extra_data_since_last_finalize)
        {
            if ((rhs.set).find(data)){
                auto it = std::find(extra_data_since_last_finalize.begin(), extra_data_since_last_finalize.end(), data);
                if (it != extra_data_since_last_finalize.end())
                {
                    extra_data_since_last_finalize.erase(it);
                } 
            }        
        }

        for (const auto & data : rhs.extra_data_since_last_finalize)
        {
            auto [_, inserted] = set.insert(data);
            if (inserted)
                extra_data_since_last_finalize.emplace_back(data);
        }
    
        set.merge(rhs.set);
        if ((rhs.extra_data_since_last_finalize).size())
        {
            uintptr_t temp = reinterpret_cast<uintptr_t>(place);
            auto find_place = std::find(related_places.begin(), related_places.end(),temp);
            if (find_place == related_places.end())
                related_places.emplace_back(temp);
        }

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
    std::vector<uintptr_t> related_places;
    bool use_extra_data = false;
    void merge(const Self & rhs, Arena * arena, ConstAggregateDataPtr __restrict place)
    {
        Set::LookupResult it;
        bool inserted;
       
        for (const auto & data : extra_data_since_last_finalize)
        {
            if ((rhs.set).find(data)){
                auto next = std::find(extra_data_since_last_finalize.begin(), extra_data_since_last_finalize.end(), data);
                if (next != extra_data_since_last_finalize.end())
                {
                    extra_data_since_last_finalize.erase(next);
                } 
            } 

        }

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
        uintptr_t temp = reinterpret_cast<uintptr_t>(place);
        auto find_place = std::find(related_places.begin(), related_places.end(),temp);
        if (find_place == related_places.end())
            related_places.emplace_back(temp);

        
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

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

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
        this->data(place).merge(this->data(rhs), arena, rhs);
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

        assert(!arguments.empty());
        nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);


        /// clear all the extra data in related blocks
        for (auto & item : this->data(place).related_places) {
            this->data(reinterpret_cast<AggregateDataPtr>(item)).extra_data_since_last_finalize.clear();
        }

        /// Add the temp data to sum so that the distinct outcome can accumulate
        if(this->data(place).related_places.size()){
            nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(reinterpret_cast<AggregateDataPtr>(this->data(place).related_places[0])), arguments_raw.data(), arena);
        }

        if (this->data(place).extra_data_since_last_finalize.size())
        {
            this->data(place).extra_data_since_last_finalize.clear();
        }

        if (this->data(place).related_places.size())
        {
            this->data(place).related_places.clear();
        }    
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
        return nested_func->getName() + "_distinct_streaming";
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
