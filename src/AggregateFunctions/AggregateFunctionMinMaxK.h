#pragma once

#include "IAggregateFunction.h"

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <any>

/// min_k/max_k(k)(compare_column(s) [,context_1, context_2 ...])
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
///     3. 18, 'bbb'
///
/// [query]
/// Only output compare columns:
/// 1) min_k(3)(id)                             ->  result: [1, 1, 2]
/// 2) min_k(3)(tuple(id, value))               ->  result: [(1,10), (1,20), (2,15)]
///
/// output compare + context columns:
/// 3) min_k(3)(id, value)                      ->  result: [(1,20), (1,10), (2,15)]
/// 4) min_k(3)(id, value, name)                ->  result: [(1,20,'hhh'), (1,10,'mmm'), (2,15,'tp')]
/// 5) min_k(3)(tuple(id, value), name)         ->  result: [(1,10,'mmm'), (1,20,'hhh'), (2,15,'tp')]
/// 6) min_k(3)(id, *)                          ->  result: [(1,1,20,'hhh'), (1,1,10,'mmm'), (2,2,15,'tp')]

namespace DB
{

constexpr size_t TOP_K_MAX_SIZE = 0xFFFFFF;

template <typename T, bool is_min>
struct AggregateFunctionMinMaxKData
{
    using Container = typename std::vector<T, AllocatorWithMemoryTracking<T>>;
    using Compare = std::function<bool(const T &, const T &)>;
    Container values;
    SpaceSavingArena<T> arena;
    Compare compare;
    bool is_sorted = false;

    AggregateFunctionMinMaxKData()
    {
        /// We ignore the Nan or NULL, so
        /// if is min_k, `nan_direction_hint=1` are considered larger than all numbers
        /// if is max_k, `nan_direction_hint=-1` are considered less than all numbers
        if constexpr (is_min)
            compare = std::bind(CompareHelper<T>::less, std::placeholders::_1, std::placeholders::_2, /* nan_direction_hint */ 1);
        else
            compare = std::bind(CompareHelper<T>::greater, std::placeholders::_1, std::placeholders::_2, /* nan_direction_hint */ -1);
        std::make_heap(values.begin(), values.end(), compare);
    }

    void add(const T & val, UInt64 k)
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

    void reserve(UInt64 k)
    {
        if (unlikely(values.capacity() < k))
            values.reserve(k);
    }

    typename Container::const_iterator begin() const { return values.begin(); }

    typename Container::const_iterator end() const { return values.end(); }

    size_t size() const { return values.size(); }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(values.size(), wb);

        for (const auto & value : values)
            writeBinary(value, wb);

        writeBoolText(is_sorted, wb);
    }

    void read(ReadBuffer & rb)
    {
        size_t size = 0;
        readVarUInt(size, rb);

        assert(size < TOP_K_MAX_SIZE);

        /// We free current values before read.
        for (const auto & value : values)
            arena.free(value);

        values.resize(size);
        for (size_t i = 0; i < size; ++i)
            readBinary(values[i], rb);

        readBoolText(is_sorted, rb);
    }

    void sort()
    {
        if (!is_sorted)
        {
            std::sort_heap(values.begin(), values.end(), compare);
            is_sorted = true;
        }
    }
};


template <typename T, bool is_min>
class AggregateFunctionMinMaxK
    : public IAggregateFunctionDataHelper<AggregateFunctionMinMaxKData<T, is_min>, AggregateFunctionMinMaxK<T, is_min>>
{
protected:
    using State = AggregateFunctionMinMaxKData<T, is_min>;
    UInt64 k;
    UInt64 reserved;

public:
    AggregateFunctionMinMaxK(UInt64 k_, const DataTypePtr & argument_type_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionMinMaxKData<T, is_min>, AggregateFunctionMinMaxK<T, is_min>>(
            {argument_type_}, params)
        , k(k_)
        , reserved(k_ + 1)
    {
    }

    String getName() const override { return is_min ? "min_k" : "max_k"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(this->argument_types[0]); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);
        top_k.add(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], k);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        for (const auto & elem : this->data(rhs))
            top_k.add(elem, k);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);
        top_k.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        auto & top_k = this->data(place);
        size_t size = top_k.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        top_k.sort();

        size_t i = 0;
        for (auto it = top_k.begin(); it != top_k.end(); ++it, ++i)
            data_to[old_size + i] = *it;
    }
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns minMaxK() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column, bool is_min>
class AggregateFunctionMinMaxKGeneric : public IAggregateFunctionDataHelper<
                                            AggregateFunctionMinMaxKData<StringRef, is_min>,
                                            AggregateFunctionMinMaxKGeneric<is_plain_column, is_min>>
{
private:
    using State = AggregateFunctionMinMaxKData<StringRef, is_min>;

    UInt64 k;
    UInt64 reserved;
    DataTypePtr & input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionMinMaxKGeneric(UInt64 k_, const DataTypePtr & input_data_type_, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregateFunctionMinMaxKData<StringRef, is_min>,
            AggregateFunctionMinMaxKGeneric<is_plain_column, is_min>>({input_data_type_}, params)
        , k(k_)
        , reserved(k_ + 1)
        , input_data_type(this->argument_types[0])
    {
    }

    String getName() const override { return is_min ? "min_k" : "max_k"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        // Specialized here because there's no deserialiser for StringRef
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            top_k.add(ref, k);
            arena->rollback(ref.size);
        }

        readBoolText(top_k.is_sorted, buf);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        if constexpr (is_plain_column)
        {
            top_k.add(columns[0]->getDataAt(row_num), k);
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            top_k.add(str_serialized, k);
            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);
        top_k.reserve(reserved);

        for (const auto & elem : this->data(rhs))
            top_k.add(elem, k);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & top_k = this->data(place);
        offsets_to.push_back(offsets_to.back() + top_k.size());

        top_k.sort();

        for (auto & elem : top_k)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.data, elem.size);
            else
                data_to.deserializeAndInsertFromArena(elem.data);
        }
    }
};

enum class TypeCategory : UInt8
{
    NUMERIC,
    STRING_REF,
    SEIRIALIZED_STRING_REF,
    OTHERS,
};

struct TupleOperators
{
    using Comparer = std::function<int(const std::any &, const std::any &)>;
    using Getter = std::function<std::pair<std::any, TypeCategory>(const IColumn **, size_t, Arena *)>;
    using Appender = std::function<void(const std::any &, ColumnTuple &)>;
    using Writer = std::function<void(const std::any &, WriteBuffer &)>;
    using Reader = std::function<std::pair<std::any, size_t /*alloc_size*/>(ReadBuffer &, Arena *)>;

    std::vector<Comparer> comparers;    /// Tuple element comparers
    std::vector<Getter> getters;        /// Tuple element retrievers
    std::vector<Appender> appenders;    /// Tuple element appender. Append one element to ColumnTuple
    std::vector<Writer> writers;        /// Tuple element writers
    std::vector<Reader> readers;        /// Tuple element readers
};

struct TupleValue
{
    std::vector<std::any> values;
    std::vector<size_t> string_ref_indexs;
    const TupleOperators * operators;

    TupleValue(std::vector<std::any> values_, std::vector<size_t> string_ref_indexs_, const TupleOperators & operators_)
        : values(std::move(values_)), string_ref_indexs(std::move(string_ref_indexs_)), operators(&operators_)
    {
    }

    bool operator<(const TupleValue & val) const
    {
        assert(operators->comparers.size() <= values.size());
        assert(operators->comparers.size() <= val.values.size());
        for (size_t i = 0; i < operators->comparers.size(); ++i)
        {
            auto res = operators->comparers[i](values[i], val.values[i]);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }

    bool operator>(const TupleValue & val) const
    {
        assert(operators->comparers.size() <= values.size());
        assert(operators->comparers.size() <= val.values.size());
        for (size_t i = 0; i < operators->comparers.size(); ++i)
        {
            auto res = operators->comparers[i](values[i], val.values[i]);
            if (res > 0)
                return true;
            else if (res < 0)
                return false;
        }
        return false;
    }

    void write(WriteBuffer & buf) const
    {
        assert(operators->writers.size() == values.size());
        for (size_t i = 0; i < operators->writers.size(); ++i)
            operators->writers[i](values[i], buf);

        writeVectorBinary(string_ref_indexs, buf);
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

inline void writeBinary(const TupleValue & x, WriteBuffer & buf)
{
    x.write(buf);
}

template <bool is_min>
struct AggregateFunctionMinMaxKTupleData : AggregateFunctionMinMaxKData<TupleValue, is_min>
{
    TupleOperators operators;

    void read(ReadBuffer & rb, Arena * arena_)
    {
        size_t size = 0;
        readVarUInt(size, rb);

        assert(size < TOP_K_MAX_SIZE);

        /// We free current values before read.
        for (const auto & value : this->values)
            this->arena.free(value);

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
            this->values.push_back(TupleValue(std::move(tuple_values), std::move(string_ref_indexs), operators));
            arena_->rollback(arena_allocated_size);
        }

        readBoolText(this->is_sorted, rb);
    }
};

template <bool is_min>
class AggregateFunctionMinMaxKTuple
    : public IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>
{
protected:
    using State = AggregateFunctionMinMaxKTupleData<is_min>;
    UInt64 k;
    UInt64 reserved;

public:
    AggregateFunctionMinMaxKTuple(UInt64 k_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionMinMaxKTupleData<is_min>, AggregateFunctionMinMaxKTuple<is_min>>(arguments, params)
        , k(k_)
        , reserved(k_ + 1)
    {
    }

protected:
    void buildTupleValueOperators(TupleOperators & operators) const;

public:
    String getName() const override { return is_min ? "min_k" : "max_k"; }

    DataTypePtr getReturnType() const override
    {
        assert(this->argument_types.size() > 1);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(this->argument_types));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & top_k = this->data(place);
        std::vector<std::any> tuple_value;
        std::vector<size_t> string_ref_indexs;
        size_t arena_allocated_size = 0;
        for (const auto & getter : top_k.operators.getters)
        {
            auto [val, type_category] = getter(columns, row_num, arena);
            if (type_category == TypeCategory::STRING_REF)
            {
                string_ref_indexs.push_back(tuple_value.size());
            }
            else if (type_category == TypeCategory::SEIRIALIZED_STRING_REF)
            {
                string_ref_indexs.push_back(tuple_value.size());
                arena_allocated_size += std::any_cast<const StringRef &>(val).size;
            }

            tuple_value.push_back(std::move(val));
        }

        top_k.add(TupleValue(std::move(tuple_value), std::move(string_ref_indexs), top_k.operators), k);
        arena->rollback(arena_allocated_size);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & top_k = this->data(place);

        for (const auto & elem : this->data(rhs))
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
        new (place) State;
        auto & data = this->data(place);
        data.reserve(reserved);
        this->buildTupleValueOperators(data.operators);
    }
};
}

#undef BUILD_COLUMN_COMPARE
