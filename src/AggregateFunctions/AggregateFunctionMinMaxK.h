#pragma once

#include "IAggregateFunction.h"

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <any>

/// min_k/max_k(compare_column(s), k, [,context_1, context_2 ...])
/// NOTE:
/// arguments:
///   - Compare: first argument is the compare column(s), which supports: single-column or multi-columns(tuple)
///   - Context: other arguments supports: omitted, single-column, multi-columns, many columns
/// Examples:
/// [table]
///    id, value, name
///     1, 20, 'hhh'
///     1, 10, 'mmm'
///     2, 15, 'tp'
///     3, 18, 'bbb'
///
/// [query]
/// Only output compare columns:
/// 1) min_k(id, 3)                             ->  result: [1, 1, 2]
/// 2) min_k(tuple(id, value),3)               ->  result: [(1,10), (1,20), (2,15)]
///
/// output compare + context columns:
/// 3) min_k(id,3,value)                      ->  result: [(1,20), (1,10), (2,15)]
/// 4) min_k(id,3,value, name)                ->  result: [(1,20,'hhh'), (1,10,'mmm'), (2,15,'tp')]
/// 5) min_k(tuple(id, value),3, name)         ->  result: [(1,10,'mmm'), (1,20,'hhh'), (2,15,'tp')]
/// 6) min_k(id,3, *)                          ->  result: [(1,20,'hhh'), (1,10,'mmm'), (2,15,'tp')]

namespace DB
{

constexpr size_t TOP_K_MAX_SIZE = 0xFFFFFF;

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns minMaxK() can be implemented more efficiently (especially for small numeric arrays).
 */
enum class TypeCategory : UInt8
{
    NUMERIC,
    STRING_REF,
    SEIRIALIZED_STRING_REF,
    DECIMAL,
    TUPLE,
    OTHERS,
};

struct TupleOperators
{
    using Comparer = std::function<int(const std::any &, const std::any &)>;
    using Getter = std::function<std::pair<std::any, TypeCategory>(const IColumn **, size_t, Arena *)>;
    using Appender = std::function<void(const std::any &, ColumnTuple &)>;
    using Writer = std::function<void(const std::any &, WriteBuffer &)>;
    using Reader = std::function<std::pair<std::any, size_t /*alloc_size*/>(ReadBuffer &, Arena *)>;

    std::vector<Comparer> comparers; /// Tuple element comparers
    std::vector<Getter> getters; /// Tuple element retrievers
    std::vector<Appender> appenders; /// Tuple element appender. Append one element to ColumnTuple
    std::vector<Writer> writers; /// Tuple element writers
    std::vector<Reader> readers; /// Tuple element readers
};

struct TupleValue
{
    std::vector<std::any> values;
    std::vector<size_t> string_ref_indexs;

    TupleValue(std::vector<std::any> values_, std::vector<size_t> string_ref_indexs_)
        : values(std::move(values_)), string_ref_indexs(std::move(string_ref_indexs_))
    {
    }
};

template <>
struct SpaceSavingArena<TupleValue>
{
    const TupleValue emplace(const TupleValue & key)
    {
        TupleValue new_tuple_value(key);
        for (auto string_ref_idx : key.string_ref_indexs)
        {
            const auto & string_ref = std::any_cast<const StringRef &>(key.values[string_ref_idx]);
            auto ptr = arena.alloc(string_ref.size);
            std::copy(string_ref.data, string_ref.data + string_ref.size, ptr);
            new_tuple_value.values[string_ref_idx] = StringRef{ptr, string_ref.size};
        }

        for (size_t index = 0; index < key.values.size(); ++index)
        {
            if (key.values[index].type() == typeid(TupleValue))
                new_tuple_value.values[index] = emplace(std::any_cast<const TupleValue &>(key.values[index]));
        }

        return new_tuple_value;
    }

    void free(const TupleValue & key)
    {
        for (auto string_ref_idx : key.string_ref_indexs)
        {
            const auto & string_ref = std::any_cast<const StringRef &>(key.values[string_ref_idx]);
            arena.free(const_cast<char *>(string_ref.data), string_ref.size);
        }
    }

private:
    ArenaWithFreeLists arena;
};

template <bool is_min>
struct AggregateFunctionMinMaxKTupleData
{
    using Container = typename std::vector<TupleValue, AllocatorWithMemoryTracking<TupleValue>>;
    using Compare = std::function<bool(const TupleValue &, const TupleValue &)>;
    Container values;
    SpaceSavingArena<TupleValue> arena;
    Compare compare;
    bool is_sorted = false;
    TupleOperators operators;

    AggregateFunctionMinMaxKTupleData(TupleOperators && operators_) : operators(operators_)
    {
        AggregateFunctionMinMaxKTupleData::compare = [&](const TupleValue & l, const TupleValue & r) -> bool {
            if constexpr (is_min)
                return operators.comparers[0](l.values[0], r.values[0]) < 0;
            else
                return operators.comparers[0](l.values[0], r.values[0]) > 0;
        };
        std::make_heap(
            AggregateFunctionMinMaxKTupleData::values.begin(),
            AggregateFunctionMinMaxKTupleData::values.end(),
            AggregateFunctionMinMaxKTupleData::compare);
    }
    AggregateFunctionMinMaxKTupleData() { }

    void read(ReadBuffer & rb, Arena * arena_)
    {
        size_t size = 0;
        readVarUInt(size, rb);

        assert(size < TOP_K_MAX_SIZE);

        /// We free current values before read.
        for (const auto & value : this->values)
            this->arena.free(value);

        this->values.clear();
        this->values.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            std::vector<std::any> tuple_values;
            tuple_values.reserve(operators.readers.size());
            size_t arena_allocated_size = 0;
            for (size_t idx = 0; idx < operators.readers.size(); ++idx)
            {
                auto [val, alloc_size] = operators.readers[idx](rb, arena_);
                tuple_values.push_back(val);
                arena_allocated_size += alloc_size;
            }

            std::vector<size_t> string_ref_indexs;
            readVectorBinary(string_ref_indexs, rb);
            this->values.push_back(TupleValue(std::move(tuple_values), std::move(string_ref_indexs)));
            arena_->rollback(arena_allocated_size);
        }

        readBoolText(this->is_sorted, rb);
    }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(values.size(), wb);

        for (const auto & value : values)
        {
            assert(operators.writers.size() == value.values.size());
            for (size_t idx = 0; idx < operators.writers.size(); ++idx)
                operators.writers[idx](value.values[idx], wb);

            writeVectorBinary(value.string_ref_indexs, wb);
        }

        writeBoolText(is_sorted, wb);
    }

    void add(const TupleValue & val, UInt64 k)
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

    void sort()
    {
        if (!is_sorted)
        {
            std::sort_heap(values.begin(), values.end(), compare);
            is_sorted = true;
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
};

template <bool is_min>
class AggregateFunctionMinMaxKTuple
    : public IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>
{
protected:
    using State = AggregateFunctionMinMaxKTupleData<is_min>;
    UInt64 k;

public:
    AggregateFunctionMinMaxKTuple(UInt64 k_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>(arguments, params)
        , k(k_)
    {
    }

protected:
    TupleOperators buildTupleValueOperators() const;

public:
    String getName() const override { return is_min ? "min_k" : "max_k"; }

    DataTypePtr getReturnType() const override
    {
        if (this->argument_types.size() == 1 && WhichDataType(this->argument_types[0]).idx != TypeIndex::Tuple)
            return std::make_shared<DataTypeArray>(this->argument_types[0]);
        else
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(this->argument_types));
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        std::vector<std::any> tuple_value;
        std::vector<size_t> string_ref_indexs;
        for (const auto & getter : top_k.operators.getters)
        {
            auto [val, type_category] = getter(columns, row_num, arena);
            if (type_category == TypeCategory::STRING_REF)
            {
                string_ref_indexs.push_back(tuple_value.size());
            }

            tuple_value.push_back(std::move(val));
        }

        top_k.add(TupleValue(std::move(tuple_value), std::move(string_ref_indexs)), k);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);

        for (const auto & elem : this->data(rhs).values)
            top_k.add(elem, k);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        top_k.read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override;

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) State(this->buildTupleValueOperators());
        auto & data = this->data(place);
        data.reserve(k + 1);
    }
};
}

#undef BUILD_COLUMN_COMPARE
