#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Streaming/CountedValueMap.h>

#include <Common/serde.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{

/// uint32 max value
constexpr uint32_t INTERNAL_MAP_SIZE = 0xFFFFFFFF;

struct AggregateFunctionDistinctRetractGenericData
{
    /// When creating, the hash table must be small.
    using Map = CountedValueMap<StringRef, false>; /// map<key(without delta_col), uint32>
    using Self = AggregateFunctionDistinctRetractGenericData;
    Map map;

    std::vector<std::pair<std::string, int8_t>> extra_data_since_last_finalize; /// first element is key, second one is delta_col

    bool use_extra_data = false; /// not used, but is kept for backward compatibility

    NO_SERDE std::vector<uintptr_t> merged_places;

    AggregateFunctionDistinctRetractGenericData() : map(INTERNAL_MAP_SIZE) { }

    void merge(const Self & rhs)
    {
        for (auto next = extra_data_since_last_finalize.begin(); next != extra_data_since_last_finalize.end();)
        {
            const auto & pairs = *next;
            if (rhs.map.contains(pairs.first))
            {
                bool is_new_data = std::find_if(
                                       (rhs.extra_data_since_last_finalize).begin(),
                                       (rhs.extra_data_since_last_finalize).end(),
                                       [pairs](const std::pair<std::string, int8_t> & elem) {
                                           return elem.first == pairs.first && elem.second == pairs.second;
                                       })
                    != (rhs.extra_data_since_last_finalize).end();
                if (is_new_data)
                    ++next;
                else
                    next = extra_data_since_last_finalize.erase(next);
            }
            else
            {
                ++next;
            }
        }

        /// Merge and deduplicate rhs' extra data
        for (const auto & [key, delta_col] : rhs.extra_data_since_last_finalize)
        {
            bool inserted = map.insert(key, delta_col);
            if (inserted)
                extra_data_since_last_finalize.emplace_back(key, delta_col);
        }

        map.merge(rhs.map);

        uintptr_t merged_place = reinterpret_cast<uintptr_t>(&rhs);
        auto find_place = std::find(merged_places.begin(), merged_places.end(), merged_place);
        if (find_place == merged_places.end())
            merged_places.emplace_back(merged_place);
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

    void deserialize(ReadBuffer & buf)
    {
        map.clear();

        size_t map_size;
        readVarUInt(map_size, buf);

        StringRef val;
        uint32_t count;
        for (size_t i = 0; i < map_size; ++i)
        {
            val = readStringBinaryInto(*map.getArena().getArenaWithFreeLists(), buf);
            readVarUInt(count, buf);
            map.insert</*preallocated=*/true>(std::move(val), count);
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
        if (is_new_inserted_key)
        {
            std::pair<std::string, int8_t> target = {key.toString(), -1};
            auto it = std::find(extra_data_since_last_finalize.begin(), extra_data_since_last_finalize.end(), target);
            if (it == extra_data_since_last_finalize.end())
            {
                extra_data_since_last_finalize.emplace_back(key.toString(), +1); /// insert a copy versioned key
            }
            else
            {
                extra_data_since_last_finalize.erase(it);
            }
        }
        /// proton: ends.
    }

    void negate(StringRef key)
    {
        /// proton: starts.
        [[maybe_unused]] bool erase_success = map.erase(key);
        assert(erase_success);
        if (!map.contains(key))
            extra_data_since_last_finalize.emplace_back(key.toString(), -1);
        /// proton: ends.
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        const size_t argument_size = argument_types.size();
        MutableColumns argument_columns(argument_size);
        for (size_t i = 0; i < argument_size; ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto & [key, delta_col] : extra_data_since_last_finalize)
        {
            /// serialize key
            const char * begin = key.c_str();
            for (size_t i = 0; i < argument_size - 1; ++i)
                begin = argument_columns[i]->deserializeAndInsertFromArena(begin);

            /// insert delta_col
            argument_columns.back()->insert(delta_col);
        }

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

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

public:
    AggregateFunctionDistinctRetract(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctRetract>(arguments, params_, nested_func_->getResultType())
        , nested_func(nested_func_)
        , arguments_num(arguments.size())
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        WriteBufferFromOwnString buf;
        /// We do not serialize the `delta_col` because it is meaningless; calling the `add()` function with only +1 is sufficient.
        for (size_t i = 0; i < arguments_num - 1; ++i)
            columns[i]->serializeValueIntoBuffer(row_num, buf);

        this->data(place).add(buf.str());
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        WriteBufferFromOwnString buf;
        for (size_t i = 0; i < arguments_num - 1; ++i)
            columns[i]->serializeValueIntoBuffer(row_num, buf);

        this->data(place).negate(buf.str());
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs));
        nested_func->merge(getNestedPlace(place), getNestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
        nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf);
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

        /// Clear all the extra data in related blocks
        for (auto & merged_place : data.merged_places)
            this->data(reinterpret_cast<AggregateDataPtr>(merged_place)).extra_data_since_last_finalize.clear();

        if (!data.merged_places.empty())
            nested_func->addBatchSinglePlace(
                0,
                arguments[0]->size(),
                getNestedPlace(reinterpret_cast<AggregateDataPtr>(data.merged_places[0])),
                arguments_raw.data(),
                arena,
                -1 /* if_argument_pos */,
                *(arguments_raw.end() - 1) /* delta_col */);

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

    String getName() const override { return nested_func->getName() + "_distinct_retract"; }

    bool allocatesMemoryInArena() const override { return nested_func->allocatesMemoryInArena(); }

    bool isState() const override { return nested_func->isState(); }

    bool isVersioned() const override { return nested_func->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_func->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_func->getDefaultVersion(); }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
}
