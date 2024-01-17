#pragma once

#include <base/StringRef.h>
#include <Common/logger_useful.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>
#include <Common/assert_cast.h>

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Aggregator.h>

/// proton: starts
#include "Common.h"

#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/TimeBucketHashMap.h>
#include <Common/ProtonCommon.h>

#include <functional>
#include <memory>
#include <mutex>
#include <numeric>
#include <ranges>
#include <shared_mutex>

/// proton: ends

namespace DB::ErrorCodes
{
extern int UNKNOWN_STREAMING_TABLE_VARIANT;
}
namespace DB::Streaming::Substream
{
template <typename C, typename Data>
concept DataCreator
    = ((std::is_void_v<Data> && std::is_invocable_v<C, const ID &>)
       || (!std::is_void_v<Data> && std::is_invocable_v<C, const ID &, char *>));

template <typename Data>
struct DataID
{
    Data & data;
    const ID id;
};

template <typename Data>
struct DataIDRows
{
    Data & data;
    const ID id;
    size_t rows;
};

struct IDRows
{
    ID id;
    size_t rows;
};

template <typename Data>
using DataWithID = std::conditional_t<std::is_void_v<Data>, const ID, DataID<Data>>;

template <typename Data>
using DataWithIDRows = std::conditional_t<std::is_void_v<Data>, IDRows, DataIDRows<Data>>;

template <typename Fn, typename Data>
concept RowTransform = (std::is_invocable_v<Fn, DataWithID<Data> &, IColumn::ColumnIndex, const Columns &>);

template <typename Fn, typename Data>
concept BatchTransform = (std::is_invocable_v<Fn, std::vector<DataWithIDRows<Data>> &, const IColumn::Selector &, const Columns &>);

template <typename Fn, typename Data>
concept BatchTransformForOneSubstream = (std::is_invocable_v<Fn, DataWithIDRows<Data> &, const Columns &>);

template <typename Fn, typename Data>
concept ColumnsTransform
    = ((RowTransform<Fn, Data> && !BatchTransform<Fn, Data> && !BatchTransformForOneSubstream<Fn, Data>)
       || (!RowTransform<Fn, Data> && BatchTransform<Fn, Data> && !BatchTransformForOneSubstream<Fn, Data>)
       || (!RowTransform<Fn, Data> && !BatchTransform<Fn, Data> && BatchTransformForOneSubstream<Fn, Data>));

template <typename Fn, typename Data>
concept CallbackDataFunc = (!std::is_void_v<Data> && std::is_invocable_v<Fn, Data &>);

template <typename Fn, typename Data>
concept CallbackValueFunc = (!std::is_void_v<Data> && std::is_invocable_v<Fn, const ID, Data &>);

template <typename Fn, typename Data>
concept DestroyDataFunc = std::is_invocable_v<Fn, Data &>;

template <typename R, typename T>
concept RangeOf
    = (std::ranges::viewable_range<
           R> && (std::same_as<std::ranges::range_value_t<R>, T> || std::derived_from<std::ranges::range_value_t<R>, T>));

template <typename R, typename T>
concept RangePtrOf = requires(std::ranges::range_value_t<R> && elem)
{
    std::ranges::viewable_range<R>;
    static_cast<const T &>(*elem);
    static_cast<const T &>(*(elem.operator->()));
};

template <typename Data = void>
struct ExecutorImpl
{
    using Self = ExecutorImpl<Data>;

    /// Keys defination
    using DataPtr = char *;

    using DataWithUInt8Key = FixedImplicitZeroHashMapWithCalculatedSize<UInt8, DataPtr>;
    using DataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, DataPtr>;

    using DataWithUInt32Key = HashMap<UInt32, DataPtr, HashCRC32<UInt32>>;
    using DataWithUInt64Key = HashMap<UInt64, DataPtr, HashCRC32<UInt64>>;

    using DataWithShortStringKey = StringHashMap<DataPtr>;

    using DataWithStringKey = HashMapWithSavedHash<StringRef, DataPtr>;

    using DataWithKeys128 = HashMap<UInt128, DataPtr, UInt128HashCRC32>;
    using DataWithKeys256 = HashMap<UInt256, DataPtr, UInt256HashCRC32>;

    /** Variants with better hash function, using more than 32 bits for hash.
    * Using for merging phase of external aggregation, where number of keys may be far greater than 4 billion,
    *  but we keep in memory and merge only sub-partition of them simultaneously.
    * TODO We need to switch for better hash function not only for external aggregation,
    *  but also for huge aggregation results on machines with terabytes of RAM.
    */
    using DataWithUInt64KeyHash64 = HashMap<UInt64, DataPtr, DefaultHash<UInt64>>;
    using DataWithStringKeyHash64 = HashMapWithSavedHash<StringRef, DataPtr, StringRefHash64>;
    using DataWithKeys128Hash64 = HashMap<UInt128, DataPtr, UInt128Hash>;
    using DataWithKeys256Hash64 = HashMap<UInt256, DataPtr, UInt256Hash>;

    template <typename... Types>
    using HashTableWithNullKey = AggregationDataWithNullKey<HashMapTable<Types...>>;
    template <typename... Types>
    using StringHashTableWithNullKey = AggregationDataWithNullKey<StringHashMap<Types...>>;

    using DataWithNullableUInt8Key = AggregationDataWithNullKey<DataWithUInt8Key>;
    using DataWithNullableUInt16Key = AggregationDataWithNullKey<DataWithUInt16Key>;

    using DataWithNullableUInt64Key = AggregationDataWithNullKey<DataWithUInt64Key>;
    using DataWithNullableStringKey = AggregationDataWithNullKey<DataWithStringKey>;

    /// Two level
    /// (Single key)
    using DataWithUInt16KeyTwoLevel = TimeBucketHashMap<UInt16, DataPtr, HashCRC32<UInt16>>;
    using DataWithUInt32KeyTwoLevel = TimeBucketHashMap<UInt32, DataPtr, HashCRC32<UInt32>>;
    using DataWithUInt64KeyTwoLevel = TimeBucketHashMap<UInt64, DataPtr, HashCRC32<UInt64>>;
    /// (Multiple keys)
    using DataWithKeys128TwoLevel = TimeBucketHashMap<UInt128, DataPtr, UInt128HashCRC32>;
    using DataWithKeys256TwoLevel = TimeBucketHashMap<UInt256, DataPtr, UInt256HashCRC32>;
    /// (Generic key)
    using DataWithStringKeyTwoLevel = TimeBucketHashMapWithSavedHash<StringRef, DataPtr>;


    // Disable consecutive key optimization for Uint8/16, because they use a FixedHashMap
    // and the lookup there is almost free, so we don't need to cache the last lookup result
    std::unique_ptr<AggregationMethodOneNumber<UInt8, DataWithUInt8Key, false>> key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, DataWithUInt16Key, false>> key16;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, DataWithUInt64Key>> key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, DataWithUInt64Key>> key64;
    std::unique_ptr<AggregationMethodStringNoCache<DataWithShortStringKey>> key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<DataWithShortStringKey>> key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithUInt16Key, false, false, false>> keys16;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithUInt32Key>> keys32;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithUInt64Key>> keys64;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128>> keys128;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256>> keys256;
    std::unique_ptr<AggregationMethodSerialized<DataWithStringKey>> serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, DataWithUInt64KeyHash64>> key64_hash64;
    std::unique_ptr<AggregationMethodString<DataWithStringKeyHash64>> key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<DataWithStringKeyHash64>> key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128Hash64>> keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256Hash64>> keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<DataWithStringKeyHash64>> serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128, true>> nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256, true>> nullable_keys256;

    /// Support for low cardinality.
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt8, DataWithNullableUInt8Key, false>>>
        low_cardinality_key8;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt16, DataWithNullableUInt16Key, false>>>
        low_cardinality_key16;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, DataWithNullableUInt64Key>>>
        low_cardinality_key32;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, DataWithNullableUInt64Key>>>
        low_cardinality_key64;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<DataWithNullableStringKey>>>
        low_cardinality_key_string;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<DataWithNullableStringKey>>>
        low_cardinality_key_fixed_string;


    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128, false, true>> low_cardinality_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256, false, true>> low_cardinality_keys256;

    /// proton: starts
    /// Single key
    std::unique_ptr<AggregationMethodOneNumber<UInt16, DataWithUInt64KeyTwoLevel>> key16_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, DataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, DataWithUInt64KeyTwoLevel>> key64_two_level;

    /// Multiple keys
    std::unique_ptr<AggregationMethodKeysFixed<DataWithUInt32KeyTwoLevel>> keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithUInt64KeyTwoLevel>> keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128TwoLevel>> keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256TwoLevel>> keys256_two_level;

    /// Nullable
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128TwoLevel, true>> nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256TwoLevel, true>> nullable_keys256_two_level;

    /// Low cardinality
    // std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, DataWithNullableUInt64KeyTwoLevel>>>  low_cardinality_key32_two_level;
    // std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, DataWithNullableUInt64KeyTwoLevel>>>  low_cardinality_key64_two_level;
    // std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<DataWithNullableStringKeyTwoLevel>>>             low_cardinality_key_string_two_level;
    // std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<DataWithNullableStringKeyTwoLevel>>>        low_cardinality_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys128TwoLevel, false, true>> low_cardinality_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<DataWithKeys256TwoLevel, false, true>> low_cardinality_keys256_two_level;

    /// Fallback
    std::unique_ptr<AggregationMethodSerialized<DataWithStringKeyTwoLevel>> serialized_two_level;
/// proton: ends

/// In this and similar macros, the option without_key is not considered.
#define APPLY_FOR_TABLE_VARIANTS(M) \
    M(key8, false) \
    M(key16, false) \
    M(key32, false) \
    M(key64, false) \
    M(key_string, false) \
    M(key_fixed_string, false) \
    M(keys16, false) \
    M(keys32, false) \
    M(keys64, false) \
    M(keys128, false) \
    M(keys256, false) \
    M(serialized, false) \
    M(key64_hash64, false) \
    M(key_string_hash64, false) \
    M(key_fixed_string_hash64, false) \
    M(keys128_hash64, false) \
    M(keys256_hash64, false) \
    M(serialized_hash64, false) \
    M(nullable_keys128, false) \
    M(nullable_keys256, false) \
    M(low_cardinality_key8, false) \
    M(low_cardinality_key16, false) \
    M(low_cardinality_key32, false) \
    M(low_cardinality_key64, false) \
    M(low_cardinality_keys128, false) \
    M(low_cardinality_keys256, false) \
    M(low_cardinality_key_string, false) \
    M(low_cardinality_key_fixed_string, false) \
    /* proton: starts */ \
    /* two level */ \
    M(key16_two_level, true) \
    M(key32_two_level, true) \
    M(key64_two_level, true) \
    M(keys32_two_level, true) \
    M(keys64_two_level, true) \
    M(keys128_two_level, true) \
    M(keys256_two_level, true) \
    M(nullable_keys128_two_level, true) \
    M(nullable_keys256_two_level, true) \
    M(low_cardinality_keys128_two_level, true) \
    M(low_cardinality_keys256_two_level, true) \
    M(serialized_two_level, true)

#define APPLY_FOR_TABLE_VARIANTS_TWO_LEVEL(M) \
    M(key16_two_level) \
    M(key32_two_level) \
    M(key64_two_level) \
    M(keys32_two_level) \
    M(keys64_two_level) \
    M(keys128_two_level) \
    M(keys256_two_level) \
    M(nullable_keys128_two_level) \
    M(nullable_keys256_two_level) \
    M(low_cardinality_keys128_two_level) \
    M(low_cardinality_keys256_two_level) \
    M(serialized_two_level)

#define APPLY_FOR_VARIANTS_LOW_CARDINALITY(M) \
    M(low_cardinality_key8, false) \
    M(low_cardinality_key16, false) \
    M(low_cardinality_key32, false) \
    M(low_cardinality_key64, false) \
    M(low_cardinality_keys128, false) \
    M(low_cardinality_keys256, false) \
    M(low_cardinality_key_string, false) \
    M(low_cardinality_key_fixed_string, false) \
    M(low_cardinality_keys128_two_level, true) \
    M(low_cardinality_keys256_two_level, true)

    /// TODO: Support for nullable keys

    /// TODO: Support for low cardinality

    enum class Type
    {
        EMPTY = 0,
#define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_TABLE_VARIANTS(M)
#undef M
    };

    bool recycleEnabled()
    {
        switch (groupby_keys_type)
        {
            case GroupByKeys::WINDOWED_PARTITION_KEYS:
                return true;
            default:
                return false;
        }
    }

    ExecutorImpl(
        Block header_,
        std::vector<size_t> keys_indices_,
        GroupByKeys groupby_keys_type_ = GroupByKeys::PARTITION_KEYS,
        const String & log_name = "SubstreamImpl")
        : log(&Poco::Logger::get(log_name))
        , groupby_keys_type(groupby_keys_type_)
        , header(std::move(header_))
        , keys_indices(std::move(keys_indices_))
        , keys_size(keys_indices.size())
        , pools(1, std::make_shared<Arena>())
        , pool(pools.back().get())
        , persisted_key_pool(std::make_shared<Arena>())
    {
        if (groupby_keys_type == GroupByKeys::WINDOWED_PARTITION_KEYS)
        {
            if (keys_indices.empty()
                || (header.getByPosition(keys_indices[0]).name != ProtonConsts::STREAMING_WINDOW_START
                    && header.getByPosition(keys_indices[0]).name != ProtonConsts::STREAMING_WINDOW_END))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invalid key(s), fail to initialize Substream Handler, first key must be {}/{}",
                    ProtonConsts::STREAMING_WINDOW_START,
                    ProtonConsts::STREAMING_WINDOW_END);
        }
        else
        {
            if (keys_indices.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No enough keys, at least 1, but actual {}", keys_size);
        }

        init(chooseMethodType(header, keys_indices));
        recycle_enabled = recycleEnabled();
        pool->enableRecycle(recycle_enabled);
    }

    ~ExecutorImpl()
    {
        /// Destroy all data.
        if constexpr (!std::is_void_v<Data>)
            destroyForEachData([](Data & data) { data.~Data(); });
    }

    bool empty() const
    {
        return type == Type::EMPTY;
    }
    void invalidate()
    {
        type = Type::EMPTY;
    }

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:
                break;
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        (NAME) = std::make_unique<typename decltype(NAME)::element_type>(); \
        break;
                APPLY_FOR_TABLE_VARIANTS(M)
#undef M
        }

        /// proton: start
        switch (type_)
        {
#define M(NAME) \
    case Type::NAME: \
        NAME->data.setWinKeySize(key_sizes[0]); \
        break;
            APPLY_FOR_TABLE_VARIANTS_TWO_LEVEL(M)
#undef M
            default:
                break;
        }
        /// proton: ends;

        HashMethodContext::Settings cache_settings;
        cache_settings.max_threads = 1;
        switch (type_)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: { \
        using TPtr##NAME = decltype(NAME); \
        using T##NAME = typename TPtr##NAME ::element_type; \
        method_ctx_ptr = T##NAME ::State::createContext(cache_settings); \
        break; \
    }
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown data variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }

        type = type_;
    }

    size_t size() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return 0;
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return (NAME)->data.size();
                APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown data variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }

        __builtin_unreachable();
    }

    const char * getMethodName() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return "EMPTY";
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return #NAME;
                APPLY_FOR_TABLE_VARIANTS(M)
#undef M
        }

        __builtin_unreachable();
    }

    bool isTwoLevel() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return false;
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return IS_TWO_LEVEL;
                APPLY_FOR_TABLE_VARIANTS(M)
#undef M
        }

        __builtin_unreachable();
    }

    bool isLowCardinality() const
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return true;
            APPLY_FOR_VARIANTS_LOW_CARDINALITY(M)
#undef M
            default:
                return false;
        }
    }

    Poco::Logger * log;
    GroupByKeys groupby_keys_type;
    bool recycle_enabled = false;
    Block header;
    std::vector<size_t> keys_indices;
    Int64 last_watermark{std::numeric_limits<Int64>::min()}; /// used for pool recycle

    Type type = Type::EMPTY;
    size_t keys_size{}; /// Number of keys. NOTE do we need this field?
    Sizes key_sizes; /// Dimensions of keys, if keys of fixed length
    Arenas pools;
    Arena * pool{};
    ArenaPtr persisted_key_pool;
    HashMethodContextPtr method_ctx_ptr;

    using DataWithID = std::conditional_t<std::is_void_v<Data>, const ID, DataID<Data>>;

    using DataWithIDRows = std::conditional_t<std::is_void_v<Data>, IDRows, DataIDRows<Data>>;

    template <DataCreator<Data> Creator, ColumnsTransform<Data>... Transform>
    void execute(const Columns & columns, size_t rows, Creator && ctor, Transform &&... transform)
    {
        /** Constant columns are not supported directly during aggregation.
        * To make them work anyway, we materialize them.
        */
        Columns materialized_columns;
        ColumnRawPtrs key_columns(keys_size, nullptr);

        /// Remember the columns we will work with
        materialized_columns.reserve(keys_size);
        for (size_t i = 0; i < keys_size; ++i)
        {
            materialized_columns.push_back(columns.at(keys_indices[i])->convertToFullColumnIfConst());
            key_columns[i] = materialized_columns.back().get();

            if (!isLowCardinality())
            {
                auto column_no_lc = recursiveRemoveLowCardinality(key_columns[i]->getPtr());
                if (column_no_lc.get() != key_columns[i])
                {
                    materialized_columns.emplace_back(std::move(column_no_lc));
                    key_columns[i] = materialized_columns.back().get();
                }
            }
        }

        if constexpr (!std::is_void_v<Data>)
        {
            if (recycle_enabled)
                recyclePoolByTimestamps(rows, key_columns);
        }

        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        executeImpl((*NAME), rows, key_columns, columns, ctor, std::forward<Transform>(transform)...); \
        break;
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }
    }

    template <typename... Fn>
    bool executeOne(const ID & id, Fn &&... f)
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return executeOneImpl((*NAME), id, std::forward<Fn>(f)...);
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }

        __builtin_unreachable();
    }

    template <typename... Fn>
    void executeForEachValue(Fn &&... f)
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        executeForEachValueImpl((*NAME), std::forward<Fn>(f)...); \
        break;
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }
    }

    template <typename... Fn>
    void executeForEachData(Fn &&... f)
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        executeForEachDataImpl((*NAME), std::forward<Fn>(f)...); \
        break;
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }
    }

    template <typename... Fn>
    void destroyForEachData(Fn &&... f)
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        destroyForEachDataImpl((*NAME), std::forward<Fn>(f)...); \
        break;
            APPLY_FOR_TABLE_VARIANTS(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }
    }

    /// Return {removed, last_removed_watermark, remaining_size}
    template <DestroyDataFunc<Data>... F>
    void removeBucketsBefore(Int64 watermark, F &&... destroy)
    {
        auto destroy_wrapper = [&](DataPtr & data) {
            if (nullptr == data)
                return;

            Data & data_ref = *reinterpret_cast<Data *>(data);
            (destroy(data_ref), ...);

            data = nullptr;
        };

        size_t removed = 0;
        Int64 last_removed_watermark = 0;
        size_t remaining = 0;

        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        std::tie(removed, last_removed_watermark, remaining) = (NAME)->data.removeBucketsBefore(watermark, destroy_wrapper); \
        break;
            APPLY_FOR_TABLE_VARIANTS_TWO_LEVEL(M)
#undef M
            default:
                throw Exception("Unknown table variant.", ErrorCodes::UNKNOWN_STREAMING_TABLE_VARIANT);
        }

        Arena::Stats stats;

        if (recycle_enabled)
            if (removed)
                stats = pool->free(last_removed_watermark);

        LOG_DEBUG(
            log,
            "Removed {} windows less or equal to watermark={}, keeping window_count=0, remaining_windows={}. "
            "Arena: arena_chunks={}, arena_size={}, chunks_removed={}, bytes_removed={}. chunks_reused={}, bytes_reused={}, "
            "head_chunk_size={}, "
            "free_list_hits={}, free_list_missed={}",
            removed,
            last_removed_watermark,
            remaining,
            stats.chunks,
            stats.bytes,
            stats.chunks_removed,
            stats.bytes_removed,
            stats.chunks_reused,
            stats.bytes_reused,
            stats.head_chunk_size,
            stats.free_list_hits,
            stats.free_list_misses);
    }

private:
    template <typename Method, DataCreator<Data> Creator, RowTransform<Data>... Transform>
    void executeImpl(
        Method & method,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Columns & columns,
        Creator && ctor,
        Transform &&... transform)
    {
        typename Method::State state(key_columns, key_sizes, method_ctx_ptr);
        constexpr bool need_data = !std::is_void_v<Data>;
        for (size_t i = 0; i < rows; ++i)
        {
            ID id = getIDImpl(method, state.getKeyHolder(i, *persisted_key_pool), persisted_key_pool);
            if constexpr (need_data)
            {
                auto emplace_result = state.emplaceKey(method.data, i, *pool);
                /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
                if (emplace_result.isInserted())
                {
                    /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                    emplace_result.setMapped(nullptr);
                    char * place = pool->alignedAlloc(sizeof(Data), alignof(Data));
                    if (ctor)
                        ctor(id, place);
                    else
                        new (place) Data{};
                    auto & data = *reinterpret_cast<Data *>(place);
                    emplace_result.setMapped(place);

                    (transform(DataWithID{data, id}, i, columns), ...);
                }
                else
                {
                    auto & data = *reinterpret_cast<Data *>(emplace_result.getMapped());
                    (transform(DataWithID{data, id}, i, columns), ...);
                }
            }
            else
                (transform(DataWithID{id}, i, columns), ...);
        }
    }

    template <typename Method, DataCreator<Data> Creator, BatchTransform<Data>... Transform>
    void executeImpl(
        Method & method,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Columns & columns,
        Creator && ctor,
        Transform &&... transform)
    {
        typename Method::State state(key_columns, key_sizes, method_ctx_ptr);
        constexpr bool need_data = !std::is_void_v<Data>;

        std::unordered_map<ID, size_t> subid_to_bukect_map;
        std::vector<DataWithIDRows> datas_with_id_rows;
        IColumn::Selector selector;
        selector.resize(rows);

        for (size_t i = 0; i < rows; ++i)
        {
            ID id = getIDImpl(method, state.getKeyHolder(i, *pool), persisted_key_pool);
            if constexpr (need_data)
            {
                Data * data = nullptr;
                auto emplace_result = state.emplaceKey(method.data, i, *pool);
                /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
                if (emplace_result.isInserted())
                {
                    /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                    emplace_result.setMapped(nullptr);
                    char * place = pool->alignedAlloc(sizeof(Data), alignof(Data));
                    if (ctor)
                        ctor(id, place);
                    else
                        new (place) Data{};
                    emplace_result.setMapped(place);
                    data = reinterpret_cast<Data *>(place);
                }
                else
                    data = reinterpret_cast<Data *>(emplace_result.getMapped());

                auto [iter, inserted] = subid_to_bukect_map.try_emplace(id, datas_with_id_rows.size());
                if (inserted)
                    datas_with_id_rows.emplace_back(DataWithIDRows{*data, id, 0});

                selector[i] = iter->second; /// set substream index of current row
                ++(datas_with_id_rows[iter->second].rows); /// ++ rows of current substream
            }
            else
            {
                auto [iter, inserted] = subid_to_bukect_map.try_emplace(id, datas_with_id_rows.size());
                if (inserted)
                    datas_with_id_rows.emplace_back(DataWithIDRows{id, 0});

                selector[i] = iter->second; /// set substream index of current row
                ++(datas_with_id_rows[iter->second].rows); /// ++ rows of current substream
            }
        }

        (transform(datas_with_id_rows, selector, columns), ...);
    }

    template <typename Method, DataCreator<Data> Creator, BatchTransformForOneSubstream<Data>... Transform>
    void executeImpl(
        Method & method,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Columns & columns,
        Creator && ctor,
        Transform &&... transform)
    {
        typename Method::State state(key_columns, key_sizes, method_ctx_ptr);
        constexpr bool need_data = !std::is_void_v<Data>;

        std::unordered_map<ID, size_t> subid_to_bukect_map;
        std::vector<DataWithIDRows> datas_with_id_rows;
        IColumn::Selector selector;
        selector.resize(rows);

        for (size_t i = 0; i < rows; ++i)
        {
            ID id = getIDImpl(method, state.getKeyHolder(i, *pool), persisted_key_pool);
            if constexpr (need_data)
            {
                Data * data = nullptr;
                auto emplace_result = state.emplaceKey(method.data, i, *pool);
                /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
                if (emplace_result.isInserted())
                {
                    /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                    emplace_result.setMapped(nullptr);
                    char * place = pool->alignedAlloc(sizeof(Data), alignof(Data));
                    if (ctor)
                        ctor(id, place);
                    else
                        new (place) Data{};
                    emplace_result.setMapped(place);
                    data = reinterpret_cast<Data *>(place);
                }
                else
                    data = reinterpret_cast<Data *>(emplace_result.getMapped());

                auto [iter, inserted] = subid_to_bukect_map.try_emplace(id, datas_with_id_rows.size());
                if (inserted)
                    datas_with_id_rows.emplace_back(DataWithIDRows{*data, id, 0});

                selector[i] = iter->second; /// set substream index of current row
                ++(datas_with_id_rows[iter->second].rows); /// ++ rows of current substream
            }
            else
            {
                auto [iter, inserted] = subid_to_bukect_map.try_emplace(id, datas_with_id_rows.size());
                if (inserted)
                    datas_with_id_rows.emplace_back(DataWithIDRows{id, 0});

                selector[i] = iter->second; /// set substream index of current row
                ++(datas_with_id_rows[iter->second].rows); /// ++ rows of current substream
            }
        }

        size_t num_substreams = datas_with_id_rows.size();
        std::vector<Columns> columns_to_substreams(num_substreams, Columns(columns.size()));
        for (size_t col_index = 0; const auto & col : columns)
        {
            auto sub_columns = col->scatter(num_substreams, selector);
            for (size_t sub_idx = 0; sub_idx < num_substreams; ++sub_idx)
                columns_to_substreams[sub_idx][col_index] = std::move(sub_columns[sub_idx]);
            ++col_index;
        }

        for (size_t sub_idx = 0; const auto & sub_columns : columns_to_substreams)
            (transform(datas_with_id_rows[sub_idx++], sub_columns), ...);
    }

    template <typename Method, CallbackDataFunc<Data>... Fn>
    bool executeOneImpl(Method & method, const ID & id, Fn &&... f)
    {
        using KeyHolder = decltype(typename Method::State{ColumnRawPtrs{}, key_sizes, method_ctx_ptr}.getKeyHolder(0, *pool));
        if constexpr (
            std::same_as<KeyHolder, ArenaKeyHolder> || std::same_as<KeyHolder, SerializedKeyHolder> || std::same_as<KeyHolder, StringRef>)
        {
            auto it = method.data.find(std::get<StringRef>(id.key));
            if (it)
            {
                auto & data = *reinterpret_cast<Data *>(it->getMapped());
                (f(data), ...);
                return true;
            }
            return false;
        }
        else
        {
            auto it = method.data.find(std::get<Field>(id.key).safeGet<KeyHolder>());
            if (it)
            {
                auto & data = *reinterpret_cast<Data *>(it->getMapped());
                (f(data), ...);
                return true;
            }
            return false;
        }
    }

    template <typename Method, CallbackValueFunc<Data>... Fn>
    void executeForEachValueImpl(Method & method, Fn &&... f)
    {
        auto func_wrapper = [&](const auto & key, auto * data) {
            if (data)
                (f(getIDRefImpl(method, key, persisted_key_pool), *reinterpret_cast<Data *>(data)), ...);
        };
        method.data.forEachValue(func_wrapper);
    }

    template <typename Method, CallbackDataFunc<Data>... Fn>
    void executeForEachDataImpl(Method & method, Fn &&... f)
    {
        auto func_wrapper = [&](auto * data) {
            if (data)
                (f(*reinterpret_cast<Data *>(data)), ...);
        };
        method.data.forEachMapped(func_wrapper);
    }

    template <typename Method, CallbackDataFunc<Data>... Fn>
    void destroyForEachDataImpl(Method & method, Fn &&... f)
    {
        auto func_wrapper = [&](auto *& data) {
            if (data)
                (f(*reinterpret_cast<Data *>(data)), ...);
            data = nullptr;
        };
        method.data.forEachMapped(func_wrapper);
    }

    template <typename Method, typename KeyHolder>
    ID getIDImpl(Method & method, KeyHolder && key_holder, ArenaPtr persisted_pool)
    {
        size_t saved_hash = method.data.hash(keyHolderGetKey(key_holder));
        if constexpr (std::is_same_v<std::decay_t<decltype(keyHolderGetKey(key_holder))>, StringRef>)
        {
            StringRef key = keyHolderGetKey(key_holder);
            constexpr bool materialized = std::same_as<KeyHolder, std::decay_t<SerializedKeyHolder>>;
            return {key, saved_hash, std::move(persisted_pool), materialized};
        }
        else
            return {Field(key_holder), saved_hash, std::move(persisted_pool), true};
    }

    Type chooseMethodType(const Block & header_, const std::vector<size_t> & keys_indices_)
    {
        keys_size = keys_indices_.size();
        assert(keys_size != 0);

        /// Check if at least one of the specified keys is nullable.
        DataTypes types_removed_nullable;
        types_removed_nullable.reserve(keys_size);
        bool has_nullable_key = false;
        bool has_low_cardinality = false;

        for (const auto & pos : keys_indices_)
        {
            DataTypePtr type_ = header_.safeGetByPosition(pos).type;

            if (type_->lowCardinality())
            {
                has_low_cardinality = true;
                type_ = removeLowCardinality(type_);
            }

            if (type_->isNullable())
            {
                has_nullable_key = true;
                type_ = removeNullable(type_);
            }

            types_removed_nullable.push_back(type_);
        }

        /** Returns ordinary (not two-level) methods, because we start from them.
        * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
        */

        size_t keys_bytes = 0;
        size_t num_fixed_contiguous_keys = 0;

        key_sizes.resize(keys_size);
        for (size_t j = 0; j < keys_size; ++j)
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            {
                if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                {
                    ++num_fixed_contiguous_keys;
                    key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                    keys_bytes += key_sizes[j];
                }
            }
        }

        /// proton: starts. Handle two level
        if (groupby_keys_type == GroupByKeys::WINDOWED_PARTITION_KEYS)
        {
            if (has_nullable_key)
            {
                if (keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
                {
                    /// Pack if possible all the keys along with information about which key values are nulls
                    /// into a fixed 16- or 32-byte blob.
                    if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                        return Type::nullable_keys128_two_level;
                    if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                        return Type::nullable_keys256_two_level;
                }

                /// Fallback case.
                return Type::serialized;
            }

            /// No key has been found to be nullable.

            /// Single numeric key.
            if (keys_size == 1)
            {
                assert(types_removed_nullable[0]->isValueRepresentedByNumber());

                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 2)
                    return Type::key16_two_level;
                if (size_of_field == 4)
                    return Type::key32_two_level;
                if (size_of_field == 8)
                    return Type::key64_two_level;

                throw Exception(
                    "Logical error: the first streaming aggregation column has sizeOfField not in 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
            }

            /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
            if (keys_size == num_fixed_contiguous_keys)
            {
                assert(keys_bytes > 2);

                if (has_low_cardinality)
                {
                    if (keys_bytes <= 16)
                        return Type::low_cardinality_keys128_two_level;
                    if (keys_bytes <= 32)
                        return Type::low_cardinality_keys256_two_level;
                }

                if (keys_bytes <= 4)
                    return Type::keys32_two_level;
                if (keys_bytes <= 8)
                    return Type::keys64_two_level;
                if (keys_bytes <= 16)
                    return Type::keys128_two_level;
                if (keys_bytes <= 32)
                    return Type::keys256_two_level;
            }

            return Type::serialized_two_level;
        }
        /// proton: ends
        else
        {
            if (has_nullable_key)
            {
                if (keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
                {
                    /// Pack if possible all the keys along with information about which key values are nulls
                    /// into a fixed 16- or 32-byte blob.
                    if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                        return Type::nullable_keys128;
                    if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                        return Type::nullable_keys256;
                }

                if (has_low_cardinality && keys_size == 1)
                {
                    if (types_removed_nullable[0]->isValueRepresentedByNumber())
                    {
                        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                        if (size_of_field == 1)
                            return Type::low_cardinality_key8;
                        if (size_of_field == 2)
                            return Type::low_cardinality_key16;
                        if (size_of_field == 4)
                            return Type::low_cardinality_key32;
                        if (size_of_field == 8)
                            return Type::low_cardinality_key64;
                    }
                    else if (isString(types_removed_nullable[0]))
                        return Type::low_cardinality_key_string;
                    else if (isFixedString(types_removed_nullable[0]))
                        return Type::low_cardinality_key_fixed_string;
                }

                /// Fallback case.
                return Type::serialized;
            }

            /// No key has been found to be nullable.

            /// Single numeric key.
            if (keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (has_low_cardinality)
                {
                    if (size_of_field == 1)
                        return Type::low_cardinality_key8;
                    if (size_of_field == 2)
                        return Type::low_cardinality_key16;
                    if (size_of_field == 4)
                        return Type::low_cardinality_key32;
                    if (size_of_field == 8)
                        return Type::low_cardinality_key64;
                }

                if (size_of_field == 1)
                    return Type::key8;
                if (size_of_field == 2)
                    return Type::key16;
                if (size_of_field == 4)
                    return Type::key32;
                if (size_of_field == 8)
                    return Type::key64;
                if (size_of_field == 16)
                    return Type::keys128;
                if (size_of_field == 32)
                    return Type::keys256;
                throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
            }

            if (keys_size == 1 && isFixedString(types_removed_nullable[0]))
            {
                if (has_low_cardinality)
                    return Type::low_cardinality_key_fixed_string;
                else
                    return Type::key_fixed_string;
            }

            /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
            if (keys_size == num_fixed_contiguous_keys)
            {
                if (has_low_cardinality)
                {
                    if (keys_bytes <= 16)
                        return Type::low_cardinality_keys128;
                    if (keys_bytes <= 32)
                        return Type::low_cardinality_keys256;
                }

                if (keys_bytes <= 2)
                    return Type::keys16;
                if (keys_bytes <= 4)
                    return Type::keys32;
                if (keys_bytes <= 8)
                    return Type::keys64;
                if (keys_bytes <= 16)
                    return Type::keys128;
                if (keys_bytes <= 32)
                    return Type::keys256;
            }

            /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
            if (keys_size == 1 && isString(types_removed_nullable[0]))
            {
                if (has_low_cardinality)
                    return Type::low_cardinality_key_string;
                else
                    return Type::key_string;
            }

            return Type::serialized;
        }
    }

    void recyclePoolByTimestamps(UInt64 num_rows, const ColumnRawPtrs & key_columns)
    {
        Int64 max_timestamp = std::numeric_limits<Int64>::min();

        /// FIXME, can we avoid this loop ?
        auto & window_col = *key_columns[0];
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto window = window_col.getInt(i);
            if (window > max_timestamp)
                max_timestamp = window;
        }

        pool->setCurrentTimestamp(max_timestamp);

        LOG_DEBUG(log, "Setup pool timestamp watermark={}", max_timestamp);

        /// When new watermark come in, we shall recycle pool by last watermark
        if (last_watermark < max_timestamp)
        {
            if (last_watermark != std::numeric_limits<Int64>::min())
                removeBucketsBefore(last_watermark);

            last_watermark = max_timestamp;
        }
    }
};
}
