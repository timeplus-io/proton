#pragma once

#include <city.h>
#include <base/bit_cast.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnDecimal.h>
#include <Common/CombinedCardinalityEstimator.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <AggregateFunctions/Streaming/CountedValueHashMap.h>

#include "config.h"

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{

/// The maximum degree of buffer size before the values are discarded
#define UNIQUES_HASH_MAX_SIZE_DEGREE 17

/// The maximum number of elements before the values are discarded
#define UNIQUES_HASH_MAX_SIZE (1ULL << (UNIQUES_HASH_MAX_SIZE_DEGREE - 1))

/// uniq
struct AggregateFunctionUniqUniquesHashSetData
{
    using Set = CountedValueHashMap<UInt64>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "unique"; }
};

/// For a function that takes multiple arguments. Such a function pre-hashes them in advance, so TrivialHash is used here.
template <bool is_exact_, bool argument_is_tuple_>
struct AggregateFunctionUniqUniquesHashSetDataForVariadic
{
    using Set = CountedValueHashMap<UInt64>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;

    static String getName() { return "unique"; }
};

/// uniq_exact

template <typename T, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactData
{
    using Set = CountedValueHashMap<T>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "unique_exact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactData<String, is_able_to_parallelize_merge_>
{
    using Set = CountedValueHashMap<UInt128>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "unique_exact"; }
};

template <bool is_exact_, bool argument_is_tuple_, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactDataForVariadic : AggregateFunctionUniqExactData<String, is_able_to_parallelize_merge_>
{
    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;
};


namespace detail
{

template <typename T>
struct IsUniqExactSet : std::false_type
{
};

template <typename T>
struct IsUniqExactSet<CountedValueHashMap<T>> : std::true_type
{
};


/** Hash function for uniq.
  */
template <typename T> struct AggregateFunctionUniqTraits
{
    static UInt64 hash(T x)
    {
        if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
        {
            return bit_cast<UInt64>(x);
        }
        else if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            return x;
        }
        else
            return DefaultHash64<T>(x);
    }
};


/** The structure for the delegation work to add elements to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename Data>
struct Adder
{
    /// We have to introduce this template parameter (and a bunch of ugly code dealing with it), because we cannot
    /// add runtime branches in whatever_hash_set::insert - it will immediately pop up in the perf top.
    template <bool use_single_level_hash_table = true>
    static void ALWAYS_INLINE add(Data & data, const IColumn ** columns, size_t num_args, size_t row_num)
    {
        if constexpr (Data::is_variadic)
        {
            if constexpr (IsUniqExactSet<typename Data::Set>::value)
                data.set.insert(UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args - 1, columns, row_num));
            else
                data.set.insert(T{UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args - 1, columns, row_num)});
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqUniquesHashSetData>)
        {
            const auto & column = *columns[0];
            if constexpr (!std::is_same_v<T, String>)
            {
                using ValueType = UInt64;
                const auto & value = assert_cast<const ColumnVector<T> &>(column).getElement(row_num);
                data.set.insert(static_cast<ValueType>(AggregateFunctionUniqTraits<T>::hash(value)));
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                data.set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T, Data::is_able_to_parallelize_merge>>)
        {
            const auto & column = *columns[0];
            if constexpr (!std::is_same_v<T, String>)
            {
                data.set.insert(assert_cast<const ColumnVector<T> &>(column).getData()[row_num]);
            }
            else
            {
                StringRef value = column.getDataAt(row_num);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key);

                data.set.insert(key);
            }
        }
#if 0
#if USE_DATASKETCHES
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqThetaData>)
        {
            const auto & column = *columns[0];
            data.set.insertOriginal(column.getDataAt(row_num));
        }
#   endif
#endif
    }

    template <bool use_single_level_hash_table = true>
    static void ALWAYS_INLINE negate(Data & data, const IColumn ** columns, size_t num_args, size_t row_num)
    {
        if constexpr (Data::is_variadic)
        {
            if constexpr (IsUniqExactSet<typename Data::Set>::value)
                data.set.erase(UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args - 1, columns, row_num));
            else
                data.set.erase(T{UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args - 1, columns, row_num)});
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqUniquesHashSetData>)
        {
           const auto & column = *columns[0];
            if constexpr (!std::is_same_v<T, String>)
            {
                using ValueType = UInt64;
                const auto & value = assert_cast<const ColumnVector<T> &>(column).getElement(row_num);
                data.set.erase(static_cast<ValueType>(AggregateFunctionUniqTraits<T>::hash(value)));
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                data.set.erase(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T, Data::is_able_to_parallelize_merge>>)
        {
            const auto & column = *columns[0];
            if constexpr (!std::is_same_v<T, String>)
            {
                data.set.erase(assert_cast<const ColumnVector<T> &>(column).getData()[row_num]);
            }
            else
            {
                StringRef value = column.getDataAt(row_num);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key);

                data.set.erase(key);
            }
        }
#if 0
#if USE_DATASKETCHES
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqThetaData>)
        {
            const auto & column = *columns[0];
            data.set.insertOriginal(column.getDataAt(row_num));
        }
#   endif
#endif
    }
                                                                                                                                                                                                     
    static void ALWAYS_INLINE
    add(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map, const IColumn * delta_col)
    {
        bool use_single_level_hash_table = true;
        const auto * delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData().data();

        if (*delta_flags == 1)
        {
            if (use_single_level_hash_table)
                addImpl<true>(data, columns, num_args, row_begin, row_end, flags, null_map);
            else
                addImpl<false>(data, columns, num_args, row_begin, row_end, flags, null_map);
        }
        else
        {
            if (use_single_level_hash_table)
                negateImpl<true>(data, columns, num_args, row_begin, row_end, flags, null_map);
            else
                negateImpl<false>(data, columns, num_args, row_begin, row_end, flags, null_map);
        }
    }

private:
    template <bool use_single_level_hash_table>
    static void ALWAYS_INLINE
    addImpl(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        if (!flags)
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    add<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
        else
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row] && flags[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
    }
    template <bool use_single_level_hash_table>
    static void ALWAYS_INLINE
    negateImpl(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        if (!flags)
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    negate<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row])
                        negate<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
        else
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row])
                        negate<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row] && flags[row])
                        negate<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
    }
};

}


/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>
{
private:
    using DataSet = typename Data::Set;
    static constexpr size_t num_args = 1;
    static constexpr bool is_able_to_parallelize_merge = Data::is_able_to_parallelize_merge;
    static constexpr bool is_parallelize_merge_prepare_needed = Data::is_parallelize_merge_prepare_needed;

public:
    explicit AggregateFunctionUniq(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_, {})
    {
    }

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_num);
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col) const override
    {
        assert(delta_col);
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */, delta_col);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, 0);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col [[maybe_unused]]) const override
    {
        assert(delta_col);
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, null_map, delta_col);
    }

    bool isParallelizeMergePrepareNeeded() const override { return is_parallelize_merge_prepare_needed;}

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        DB::writeVarUInt(this->data(place).set.size(), buf);

        for (const auto & [key, data] : this->data(place).set)
        {
            DB::writeIntBinary(key, buf);
            DB::writeIntBinary(data, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).set.clear();
        auto & set = this->data(place).set;

        size_t set_size;
        DB::readVarUInt(set_size, buf);

        if (set_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        using SetType = std::decay_t<decltype(set)>;
        using KeyType = typename SetType::key_type;
        for (size_t i = 0; i < set_size; ++i)
        {
            KeyType key;
            readIntBinary(key, buf);
            uint32_t data = 0;
            readIntBinary(data, buf);
            set.emplace(key);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of efficient implementation), you can not pass several arguments, among which there are tuples.
  */
template <typename Data>
class AggregateFunctionUniqVariadic final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data>>
{
private:
    using T = UInt64;

    static constexpr size_t is_able_to_parallelize_merge = Data::is_able_to_parallelize_merge;
    static constexpr size_t argument_is_tuple = Data::argument_is_tuple;

    size_t num_args = 0;

public:
    explicit AggregateFunctionUniqVariadic(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data>>(arguments, {})
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_num);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col [[maybe_unused]]) const override
    {
        assert(delta_col);
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */, delta_col);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col [[maybe_unused]]) const override
    {
        assert(delta_col);
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, null_map, delta_col);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        DB::writeVarUInt(this->data(place).set.size(), buf);

        for (const auto & [key, data] : this->data(place).set)
        {
            DB::writeIntBinary(key, buf);
            DB::writeIntBinary(data, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).set.clear();
        auto & set = this->data(place).set;

        size_t set_size;
        DB::readVarUInt(set_size, buf);

        if (set_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        using SetType = std::decay_t<decltype(set)>;
        using KeyType = typename SetType::key_type;
        for (size_t i = 0; i < set_size; ++i)
        {
            KeyType key;
            readIntBinary(key, buf);
            uint32_t data = 0;
            readIntBinary(data, buf);
            set.emplace(key);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};
}
}
