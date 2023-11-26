#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Streaming/CountedValueMap.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <any>

/// min_k/max_k(compare_column(s), k, [,context_1, context_2 ...])
/// NOTE:
/// arguments:
///   - Compare: first argument is the compare column(s), which supports: single-column or multi-columns(tuple)
///   - Context: other arguments supports: omitted, single-column, multi-columns, many columns
/// Examples:
/// [table]
///    id, value, name, _tp_delta
///     1, 20, 'hhh',   +1
///     1, 10, 'mmm',   +1
///     2, 15, 'tp',    +1
///     3, 18, 'bbb',   +1
///     4, 30, 'ccc',   +1
///     4, 30, 'ccc',   -1
///
/// [query]
/// Only output compare columns:
/// 1) min_k(id, 3, _tp_delta)                             ->  result: [1, 1, 2]
/// 2) min_k(tuple(id, value), 3, _tp_delta)               ->  result: [(1,10), (1,20), (2,15)]
///
/// output compare + context columns:
/// 3) min_k(id, 3, value, _tp_delta)                      ->  result: [(1,20), (1,10), (2,15)]
/// 4) min_k(id, 3, value, name, _tp_delta)                ->  result: [(1,20,'hhh'), (1,10,'mmm'), (2,15,'tp')]
/// 5) min_k(tuple(id, value), 3, name, _tp_delta)         ->  result: [(1,10,'mmm'), (1,20,'hhh'), (2,15,'tp')]

namespace DB
{

namespace Streaming
{
constexpr size_t TOP_K_MAX_SIZE = 0xFFFFFF;

struct TupleOperators
{
    using Comparer = std::function<int(const std::any &, const std::any &)>;
    using Getter = std::function<std::pair<std::any, /*is_string_ref*/ bool>(const IColumn **, size_t)>;
    using Appender = std::function<void(const std::any &, ColumnTuple &)>;
    using Writer = std::function<void(const std::any &, WriteBuffer &)>;
    using Reader = std::function<std::any(ReadBuffer &, ArenaWithFreeLists *)>;

    std::vector<Comparer> comparers; /// Tuple element comparers
    std::vector<Getter> getters; /// Tuple element retrievers
    std::vector<Appender> appenders; /// Tuple element appender. Append one element to ColumnTuple
    std::vector<Writer> writers; /// Tuple element writers
    std::vector<Reader> readers; /// Tuple element readers
};

struct TupleValue
{
    std::vector<std::any> values;
    std::vector<size_t> string_ref_indexs; /// need to alloc memory

    TupleValue(std::vector<std::any> values_, std::vector<size_t> string_ref_indexs_)
        : values(std::move(values_)), string_ref_indexs(std::move(string_ref_indexs_))
    {
    }
};

template <>
struct CountedValueArena<TupleValue>
{
    CountedValueArena() = default;
    TupleValue emplace(const TupleValue & key)
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

        for (auto & value : key.values)
        {
            if (value.type() == typeid(TupleValue))
                free(std::any_cast<const TupleValue &>(value));
        }
    }

    ArenaWithFreeLists * getArenaWithFreeLists() { return &arena; }

private:
    ArenaWithFreeLists arena;
};

template <bool is_min>
struct AggregateFunctionMinMaxKTupleData
{
    using Compare = std::function<bool(const TupleValue &, const TupleValue &)>;
    CountedValueMap<TupleValue, !is_min, Compare> values;
    TupleOperators operators;

    Compare getCompareFunc() const
    {
        return [&](const TupleValue & l, const TupleValue & r) -> bool {
            assert(operators.comparers.size() == l.values.size());
            assert(operators.comparers.size() == r.values.size());
            for (size_t i = 0; i < operators.comparers.size(); ++i)
            {
                auto ret = operators.comparers[i](l.values[i], r.values[i]);
                if (ret == 0)
                    continue;

                if constexpr (is_min)
                    return ret < 0;
                else
                    return ret > 0;
            }
            return false;
        };
    }

    AggregateFunctionMinMaxKTupleData() = default;
    AggregateFunctionMinMaxKTupleData(UInt64 max_size, TupleOperators && operators_)
        : values(max_size, getCompareFunc()), operators(operators_)
    {
    }

    void write(WriteBuffer & wb) const
    {
        /// Write size first
        writeVarUInt(values.size(), wb);

        /// Then write value / count
        for (auto [value, count] : values)
        {
            assert(operators.writers.size() == value.values.size());
            for (size_t idx = 0; idx < operators.writers.size(); ++idx)
                operators.writers[idx](value.values[idx], wb);

            writeVectorBinary(value.string_ref_indexs, wb);

            writeVarUInt(count, wb);
        }
    }

    void read(ReadBuffer & rb)
    {
        /// Clear current values before read.
        values.clear();

        size_t size = 0;
        readVarUInt(size, rb);

        assert(size < TOP_K_MAX_SIZE);

        values.setCapacity(std::max(size, values.capacity()));

        for (size_t i = 0; i < size; ++i)
        {
            std::vector<std::any> tuple_values;
            tuple_values.reserve(operators.readers.size());
            for (size_t idx = 0; idx < operators.readers.size(); ++idx)
            {
                auto val = operators.readers[idx](rb, values.getArena().getArenaWithFreeLists());
                tuple_values.push_back(std::move(val));
            }

            std::vector<size_t> string_ref_indexs;
            readVectorBinary(string_ref_indexs, rb);

            UInt32 count;
            readVarUInt(count, rb);
            [[maybe_unused]] auto inserted = values.insert(TupleValue(std::move(tuple_values), std::move(string_ref_indexs)), count);
            assert(inserted);
        }
    }

    void add(const TupleValue & val) { values.insert(val); }
    void negate(const TupleValue & val) { values.erase(val); }

    auto begin() const { return values.begin(); }
    auto end() const { return values.end(); }

    size_t size() const { return values.size(); }
};

template <bool is_min>
class AggregateFunctionMinMaxKTuple
    : public IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>
{
protected:
    using State = AggregateFunctionMinMaxKTupleData<is_min>;
    UInt64 k;
    UInt64 max_size;

public:
    AggregateFunctionMinMaxKTuple(UInt64 k_, UInt64 max_size_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>(arguments, params)
        , k(k_)
        , max_size(max_size_)
    {
    }

protected:
    TupleOperators buildTupleValueOperators() const;

public:
    String getName() const override { return is_min ? "min_k" : "max_k"; }

    DataTypePtr getReturnType() const override
    {
        if (this->argument_types.size() == 2 && !isTuple(this->argument_types[0]))
            return std::make_shared<DataTypeArray>(this->argument_types[0]);
        else
        {
            auto arg_types = this->argument_types;
            arg_types.pop_back(); /// remove _tp_delta
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(arg_types)));
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & counted_map = this->data(place);
        std::vector<std::any> tuple_value;
        std::vector<size_t> string_ref_indexs;
        for (const auto & getter : counted_map.operators.getters)
        {
            auto [val, is_string_ref] = getter(columns, row_num);
            if (is_string_ref)
                string_ref_indexs.push_back(tuple_value.size());

            tuple_value.push_back(std::move(val));
        }

        counted_map.add(TupleValue(std::move(tuple_value), std::move(string_ref_indexs)));
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & counted_map = this->data(place);
        std::vector<std::any> tuple_value;
        std::vector<size_t> string_ref_indexs;
        for (const auto & getter : counted_map.operators.getters)
        {
            auto [val, is_string_ref] = getter(columns, row_num);
            if (is_string_ref)
                string_ref_indexs.push_back(tuple_value.size());

            tuple_value.push_back(std::move(val));
        }

        counted_map.negate(TupleValue(std::move(tuple_value), std::move(string_ref_indexs)));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).values.merge(this->data(rhs).values);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override;

    void create(AggregateDataPtr __restrict place) const override { new (place) State(max_size, this->buildTupleValueOperators()); }
};
}
}

#undef BUILD_COLUMN_COMPARE
