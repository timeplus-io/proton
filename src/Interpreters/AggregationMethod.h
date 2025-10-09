#pragma once
#include <vector>
#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/ColumnsHashing.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

namespace DB
{
class IColumn;
/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData, bool consecutive_keys_optimization = true, bool nullable = false>
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodOneNumber() = default;

    explicit AggregationMethodOneNumber(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodOneNumber(const Other & other) : data(other.data)
    {
    }

    /// To use one `Method` in different threads, use different `State`.
    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodOneNumber<
        typename Data::value_type,
        Mapped,
        FieldType,
        use_cache && consecutive_keys_optimization,
        /*need_offset=*/false,
        nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    /// Use optimization for low cardinality.
    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    // Insert the key from the hash table into columns.
    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & /*key_sizes*/)
    {
        ColumnFixedSizeHelper * column;
        if constexpr (nullable)
        {
            ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[0]);
            ColumnUInt8 * null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            null_map->insertDefault();
            column = static_cast<ColumnFixedSizeHelper *>(&nullable_col.getNestedColumn());
        }
        else
        {
            column = static_cast<ColumnFixedSizeHelper *>(key_columns[0]);
        }
        static_assert(sizeof(FieldType) <= sizeof(Key));
        const auto * key_holder = reinterpret_cast<const char *>(&key);
        if constexpr (sizeof(FieldType) < sizeof(Key) && std::endian::native == std::endian::big)
            column->insertRawData<sizeof(FieldType)>(key_holder + (sizeof(Key) - sizeof(FieldType)));
        else
            column->insertRawData<sizeof(FieldType)>(key_holder);
    }
};

/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodString() = default;

    template <typename Other>
    explicit AggregationMethodString(const Other & other) : data(other.data)
    {
    }

    explicit AggregationMethodString(size_t size_hint) : data(size_hint) { }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, /*place_string_to_arena=*/true, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data, key.size);
    }
};

/// Same as above but without cache
template <typename TData, bool nullable = false>
struct AggregationMethodStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodStringNoCache() = default;

    explicit AggregationMethodStringNoCache(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodStringNoCache(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false, false, nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        if constexpr (nullable)
        {
            ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*key_columns[0]);
            assert_cast<ColumnString &>(column_nullable.getNestedColumn()).insertData(key.data, key.size);
            column_nullable.getNullMapData().push_back(0);
        }
        else
        {
            assert_cast<ColumnString &>(*key_columns[0]).insertData(key.data, key.size);
        }
    }
};

/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedString() = default;

    explicit AggregationMethodFixedString(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodFixedString(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, /*place_string_to_arena=*/true, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        assert_cast<ColumnFixedString &>(*key_columns[0]).insertData(key.data, key.size);
    }
};

/// Same as above but without cache
template <typename TData, bool nullable = false>
struct AggregationMethodFixedStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedStringNoCache() = default;

    explicit AggregationMethodFixedStringNoCache(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodFixedStringNoCache(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, true, false, false, nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        if constexpr (nullable)
            assert_cast<ColumnNullable &>(*key_columns[0]).insertData(key.data, key.size);
        else
            assert_cast<ColumnFixedString &>(*key_columns[0]).insertData(key.data, key.size);
    }
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct AggregationMethodSingleLowCardinalityColumn : public SingleColumnMethod
{
    using Base = SingleColumnMethod;
    using Data = typename Base::Data;
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using Base::data;

    template <bool use_cache>
    using BaseStateImpl = typename Base::template StateImpl<use_cache>;

    AggregationMethodSingleLowCardinalityColumn() = default;

    template <typename Other>
    explicit AggregationMethodSingleLowCardinalityColumn(const Other & other) : Base(other)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodSingleLowCardinalityColumn<BaseStateImpl<use_cache>, Mapped, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = true;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns_low_cardinality, const Sizes & /*key_sizes*/)
    {
        auto * col = assert_cast<ColumnLowCardinality *>(key_columns_low_cardinality[0]);

        if constexpr (std::is_same_v<Key, StringRef>)
            col->insertData(key.data, key.size);
        else
            col->insertData(reinterpret_cast<const char *>(&key), sizeof(key));
    }
};

/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false, bool has_low_cardinality_ = false, bool consecutive_keys_optimization = false>
struct AggregationMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;
    static constexpr bool has_low_cardinality = has_low_cardinality_;

    Data data;

    AggregationMethodKeysFixed() = default;

    explicit AggregationMethodKeysFixed(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodKeysFixed(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodKeysFixed<
        typename Data::value_type,
        Key,
        Mapped,
        has_nullable_keys,
        has_low_cardinality,
        use_cache && consecutive_keys_optimization>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        return State::shuffleKeyColumns(key_columns, key_sizes);
    }

    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        size_t keys_size = key_columns.size();

        static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = bitmap_size;

        for (size_t i = 0; i < keys_size; ++i)
        {
            IColumn * observed_column;
            ColumnUInt8 * null_map;

            bool column_nullable = false;
            if constexpr (has_nullable_keys)
                column_nullable = isColumnNullable(*key_columns[i]);

            /// If we have a nullable column, get its nested column and its null map.
            if (column_nullable)
            {
                ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[i]);
                observed_column = &nullable_col.getNestedColumn();
                null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            }
            else
            {
                observed_column = key_columns[i];
                null_map = nullptr;
            }

            bool is_null = false;
            if (column_nullable)
            {
                /// The current column is nullable. Check if the value of the
                /// corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                UInt8 val = (reinterpret_cast<const UInt8 *>(&key)[bucket] >> offset) & 1;
                null_map->insertValue(val);
                is_null = val == 1;
            }

            if (has_nullable_keys && is_null)
                observed_column->insertDefault();
            else
            {
                size_t size = key_sizes[i];
                size_t offset_to = pos;
                if constexpr (std::endian::native == std::endian::big)
                    offset_to = sizeof(Key) - size - pos;
                observed_column->insertData(reinterpret_cast<const char *>(&key) + offset_to, size);
                pos += size;
            }
        }
    }
};

/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData, bool nullable = false, bool prealloc = false>
struct AggregationMethodSerialized
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodSerialized() = default;

    explicit AggregationMethodSerialized(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodSerialized(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped, nullable, prealloc>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        const auto * pos = key.data;
        for (auto & column : key_columns)
            pos = column->deserializeAndInsertFromArena(pos);
    }
};

template <typename TData>
using AggregationMethodNullableSerialized = AggregationMethodSerialized<TData, true>;

template <typename TData>
using AggregationMethodPreallocSerialized = AggregationMethodSerialized<TData, false, true>;

template <typename TData>
using AggregationMethodNullablePreallocSerialized = AggregationMethodSerialized<TData, true, true>;


}