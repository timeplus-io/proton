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

template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;
    Set set;

    /// proton: starts. Resolve multiple finalizations problem for streaming global aggreagtion query
    std::vector<T> extra_data_since_last_finalize;
    bool use_extra_data = false;  /// Optimized, only streaming global aggreagtion query need to use extra data after first finalization.
    /// proton: ends.

    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        /// proton: starts.
        auto [_, inserted] = set.insert(vec[row_num]);
        if (use_extra_data && inserted)
            extra_data_since_last_finalize.emplace_back(vec[row_num]);
        /// proton: ends.
    }

    void merge(const Self & rhs, Arena *)
    {
        /// proton: starts.
        if (rhs.use_extra_data)
        {
            for (const auto & data : rhs.extra_data_since_last_finalize)
            {
                auto [_, inserted] = set.insert(data);
                if (use_extra_data && inserted)
                    extra_data_since_last_finalize.emplace_back(data);
            }
        }
        else if (use_extra_data)
        {
            for (const auto & elem : rhs.set)
            {
                auto [_, inserted] = set.insert(elem.getValue());
                if (inserted)
                    extra_data_since_last_finalize.emplace_back(elem.getValue());
            }
        }
        else
        {
            set.merge(rhs.set);
        }
        /// proton: ends.
    }

    void serialize(WriteBuffer & buf) const
    {
        set.write(buf);
        /// proton: starts.
        writeVectorBinary(extra_data_since_last_finalize, buf);
        writeBoolText(use_extra_data, buf);
        /// proton: ends.
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        set.read(buf);
        /// proton: starts.
        readVectorBinary(extra_data_since_last_finalize, buf);
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
            for (const auto & data : extra_data_since_last_finalize)
                argument_columns[0]->insert(data);
        }
        else
        {
            for (const auto & elem : set)
                argument_columns[0]->insert(elem.getValue());
        }
        /// proton: ends.

        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;
    Set set;
    /// proton: starts. Resolve multiple finalizations problem for streaming global aggreagtion query
    std::vector<StringRef> extra_data_since_last_finalize;
    bool use_extra_data = false;  /// Optimized, only streaming global aggreagtion query need to use extra data after first finalization.
    /// proton: ends.

    void merge(const Self & rhs, Arena * arena)
    {
        Set::LookupResult it;
        bool inserted;
        for (const auto & elem : rhs.set)
        /// proton: starts.
        {
            set.emplace(ArenaKeyHolder{elem.getValue(), *arena}, it, inserted);

            if (use_extra_data && inserted)
            {
                assert(it);
                extra_data_since_last_finalize.emplace_back(it->getValue());
            }
        }
        /// proton: ends.
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(set.size(), buf);
        for (const auto & elem : set)
            writeStringBinary(elem.getValue(), buf);

        /// proton: starts.
        writeVarUInt(extra_data_since_last_finalize.size(), buf);
        for (const auto & data : extra_data_since_last_finalize)
            writeStringBinary(data, buf);

        writeBoolText(use_extra_data, buf);
        /// proton: ends.
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));

        /// proton: starts.
        size_t extra_size;
        readVarUInt(extra_size, buf);
        extra_data_since_last_finalize.resize(extra_size);
        for (size_t i = 0; i < extra_size; ++i)
            extra_data_since_last_finalize[i] = readStringBinaryInto(*arena, buf);

        readBoolText(use_extra_data, buf);
        /// proton: ends.
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

        /// proton: starts.
        if (use_extra_data && inserted)
        {
            assert(it);
            extra_data_since_last_finalize.emplace_back(it->getValue());
        }
        /// proton: ends.
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        /// proton: starts.
        if (use_extra_data)
        {
            for (const auto & data : extra_data_since_last_finalize)
                deserializeAndInsert<is_plain_column>(data, *argument_columns[0]);
        }
        else
        {
            for (const auto & elem : set)
                deserializeAndInsert<is_plain_column>(elem.getValue(), *argument_columns[0]);
        }
        /// proton: ends.

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

        /// proton: starts.
        if (use_extra_data && inserted)
        {
            assert(it);
            extra_data_since_last_finalize.emplace_back(it->getValue());
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
            for (const auto & data : extra_data_since_last_finalize)
            {
                const char * begin = data.data;
                for (auto & column : argument_columns)
                    begin = column->deserializeAndInsertFromArena(begin);
            }
        }
        else
        {
            for (const auto & elem : set)
            {
                const char * begin = elem.getValue().data;
                for (auto & column : argument_columns)
                    begin = column->deserializeAndInsertFromArena(begin);
            }
        }
        /// proton: ends.

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

        assert(!arguments.empty());
        nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);

        /// proton: starts. Next finalization will use extra data, used in streaming global aggregation query.
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
