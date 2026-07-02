#pragma once

#include <Interpreters/Aggregator.h> /// Historical Aggregator, some definitions are from there, fixme, remove this depedency

#include <Interpreters/Streaming/Aggregator/AggregatingConvertType.h>
#include <Interpreters/Streaming/Aggregator/IAggregatedDataVariants.h>
#include <Common/HashTable/TimeBucketHashMap.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace Streaming
{

/// using TimeBucketAggregatedDataWithUInt16Key = TimeBucketHashMap<FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>>;
/// using TimeBucketAggregatedDataWithUInt32Key = TimeBucketHashMap<HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>>;
/// using TimeBucketAggregatedDataWithUInt64Key = TimeBucketHashMap<HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>>;
/// using TimeBucketAggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
/// using TimeBucketAggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

/// Single key
using TimeBucketAggregatedDataWithUInt16KeyTwoLevel = TimeBucketHashMap<UInt16, AggregateDataPtr, HashCRC32<UInt16>>;
using TimeBucketAggregatedDataWithUInt32KeyTwoLevel = TimeBucketHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using TimeBucketAggregatedDataWithUInt64KeyTwoLevel = TimeBucketHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using TimeBucketAggregatedDataWithStringKeyTwoLevel = TimeBucketHashMapWithSavedHash<StringRef, AggregateDataPtr>;

/// Multiple keys
using TimeBucketAggregatedDataWithKeys128TwoLevel = TimeBucketHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using TimeBucketAggregatedDataWithKeys256TwoLevel = TimeBucketHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using TimeBucketAggregatedDataWithKeys128TwoLevelNullable = TimeBucketHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using TimeBucketAggregatedDataWithKeys256TwoLevelNullable = TimeBucketHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;


class MemoryAggregator;

SERDE struct MemoryAggregatedDataVariants final : public IAggregatedDataVariants
{
    explicit MemoryAggregatedDataVariants(std::string id_);
    ~MemoryAggregatedDataVariants() override;

    std::string_view getID() const noexcept override { return id; }
    bool empty() const noexcept override { return type == Type::EMPTY; }
    AggregatorType aggregatorType() const noexcept override { return AggregatorType::Memory; }

    void serialize(WriteBuffer & wb, const IAggregator & aggregator_) const override;
    void deserialize(ReadBuffer & rb, const IAggregator & aggregator_) override;

    std::string id;

    /** Working with states of aggregate functions in the pool is arranged in the following (inconvenient) way:
      * - when aggregating, states are created in the pool using IAggregateFunction::create (inside - `placement new` of arbitrary structure);
      * - they must then be destroyed using IAggregateFunction::destroy (inside - calling the destructor of arbitrary structure);
      * - if aggregation is complete, then, in the Aggregator::convertToBlocks function, pointers to the states of aggregate functions
      *   are written to ColumnAggregateFunction; ColumnAggregateFunction "acquires ownership" of them, that is - calls `destroy` in its destructor.
      * - if during the aggregation, before call to Aggregator::convertToBlocks, an exception was thrown,
      *   then the states of aggregate functions must still be destroyed,
      *   otherwise, for complex states (eg, AggregateFunctionUniq), there will be memory leaks;
      * - in this case, to destroy states, the destructor calls Aggregator::destroyAggregateStates method,
      *   but only if the variable aggregator (see below) is not nullptr;
      * - that is, until you transfer ownership of the aggregate function states in the ColumnAggregateFunction, set the variable `aggregator`,
      *   so that when an exception occurs, the states are correctly destroyed.
      *
      * PS. This can be corrected by making a pool that knows about which states of aggregate functions and in which order are put in it, and knows how to destroy them.
      * But this can hardly be done simply because it is planned to put variable-length strings into the same pool.
      * In this case, the pool will not be able to know with what offsets objects are stored.
      */
    const MemoryAggregator * aggregator = nullptr;

    size_t keys_size{}; /// Number of keys. NOTE do we need this field?
    Sizes key_sizes; /// Dimensions of keys, if keys of fixed length
    std::optional<size_t> key_offset;

    /// Pools for states of aggregate functions. Ownership will be later transferred to ColumnAggregateFunction.
    /// 1) Global arena. Used for situations other than two-level time bucket hashtables
    ArenaPtr aggregates_pool;
    /// 2) Retract arena. Use separate pool to manage retract data, which will be cleared after each finalization
    ArenaPtr retract_pool;
    /// 3) Time bucket two level arenas. Used for Two level hashtable. Every hash table will
    /// use the corresponding arena in this two level arenas. And when garbage collecting
    /// the time bucket, the expired arena and hash table will be removed altogether
    std::map<Int64, ArenaPtr> time_bucket_arenas;

    /** Specialization for the case when there are no keys, and for keys not fitted into max_rows_to_group_by.
      */
    AggregatedDataWithoutKey without_key = nullptr;

    // Disable consecutive key optimization for Uint8/16, because they use a FixedHashMap
    // and the lookup there is almost free, so we don't need to cache the last lookup result
    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>> key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>> key16;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>> key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>> key64;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>> key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>> key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt16Key, false, false, false>> keys16;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>> keys32;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>> keys64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>> keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>> keys256;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>> serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>> key64_two_level;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>> key_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>> key_fixed_string_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>> keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>> keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>> keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>> keys256_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>> serialized_two_level;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>> key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>> key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>> key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>> keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>> keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>> serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>> nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>> nullable_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>> nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>> nullable_keys256_two_level;

    /// Support for low cardinality.
    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>>>
        low_cardinality_key8;
    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>>>
        low_cardinality_key16;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64Key>>>
        low_cardinality_key32;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>>
        low_cardinality_key64;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKey>>>
        low_cardinality_key_string;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKey>>>
        low_cardinality_key_fixed_string;

    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64KeyTwoLevel>>>
        low_cardinality_key32_two_level;
    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel>>>
        low_cardinality_key64_two_level;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKeyTwoLevel>>>
        low_cardinality_key_string_two_level;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKeyTwoLevel>>>
        low_cardinality_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, false, true>> low_cardinality_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, false, true>> low_cardinality_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, false, true>> low_cardinality_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, false, true>> low_cardinality_keys256_two_level;

    /// proton: starts
    /// Single key
    std::unique_ptr<AggregationMethodOneNumber<UInt16, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key16_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key64_two_level;

    /// Multiple keys
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithUInt32KeyTwoLevel>> time_bucket_keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevel>> time_bucket_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevel>> time_bucket_keys256_two_level;

    /// Nullable
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevelNullable, true>>
        time_bucket_nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevelNullable, true>>
        time_bucket_nullable_keys256_two_level;

    /// Low cardinality
    //    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, StreamingAggregatedDataWithNullableUInt64KeyTwoLevel>>> streaming_low_cardinality_key32_two_level;
    //    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, StreamingAggregatedDataWithNullableUInt64KeyTwoLevel>>> streaming_low_cardinality_key64_two_level;
    //    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<StreamingAggregatedDataWithNullableStringKeyTwoLevel>>> streaming_low_cardinality_key_string_two_level;
    //    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<StreamingAggregatedDataWithNullableStringKeyTwoLevel>>> streaming_low_cardinality_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevel, false, true>>
        time_bucket_low_cardinality_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevel, false, true>>
        time_bucket_low_cardinality_keys256_two_level;

    /// Fallback
    std::unique_ptr<AggregationMethodSerialized<TimeBucketAggregatedDataWithStringKeyTwoLevel>> time_bucket_serialized_two_level;
/// proton: ends

/// In this and similar macros, the option without_key is not considered.
#define APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M) \
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
    M(key32_two_level, true) \
    M(key64_two_level, true) \
    M(key_string_two_level, true) \
    M(key_fixed_string_two_level, true) \
    M(keys32_two_level, true) \
    M(keys64_two_level, true) \
    M(keys128_two_level, true) \
    M(keys256_two_level, true) \
    M(serialized_two_level, true) \
    M(key64_hash64, false) \
    M(key_string_hash64, false) \
    M(key_fixed_string_hash64, false) \
    M(keys128_hash64, false) \
    M(keys256_hash64, false) \
    M(serialized_hash64, false) \
    M(nullable_keys128, false) \
    M(nullable_keys256, false) \
    M(nullable_keys128_two_level, true) \
    M(nullable_keys256_two_level, true) \
    M(low_cardinality_key8, false) \
    M(low_cardinality_key16, false) \
    M(low_cardinality_key32, false) \
    M(low_cardinality_key64, false) \
    M(low_cardinality_keys128, false) \
    M(low_cardinality_keys256, false) \
    M(low_cardinality_key_string, false) \
    M(low_cardinality_key_fixed_string, false) \
    M(low_cardinality_key32_two_level, true) \
    M(low_cardinality_key64_two_level, true) \
    M(low_cardinality_keys128_two_level, true) \
    M(low_cardinality_keys256_two_level, true) \
    M(low_cardinality_key_string_two_level, true) \
    M(low_cardinality_key_fixed_string_two_level, true) \
    /* proton: starts */ \
    /* time bucket two level */ \
    M(time_bucket_key16_two_level, true) \
    M(time_bucket_key32_two_level, true) \
    M(time_bucket_key64_two_level, true) \
    M(time_bucket_keys32_two_level, true) \
    M(time_bucket_keys64_two_level, true) \
    M(time_bucket_keys128_two_level, true) \
    M(time_bucket_keys256_two_level, true) \
    M(time_bucket_nullable_keys128_two_level, true) \
    M(time_bucket_nullable_keys256_two_level, true) \
    M(time_bucket_low_cardinality_keys128_two_level, true) \
    M(time_bucket_low_cardinality_keys256_two_level, true) \
    M(time_bucket_serialized_two_level, true) \
    /* proton: ends. */

    enum class Type
    {
        EMPTY = 0,
        without_key,

#define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    };
    Type type = Type::EMPTY;

    void invalidate() { type = Type::EMPTY; }

#define APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M) \
    M(key32_two_level) \
    M(key64_two_level) \
    M(key_string_two_level) \
    M(key_fixed_string_two_level) \
    M(keys32_two_level) \
    M(keys64_two_level) \
    M(keys128_two_level) \
    M(keys256_two_level) \
    M(serialized_two_level) \
    M(nullable_keys128_two_level) \
    M(nullable_keys256_two_level) \
    M(low_cardinality_key32_two_level) \
    M(low_cardinality_key64_two_level) \
    M(low_cardinality_keys128_two_level) \
    M(low_cardinality_keys256_two_level) \
    M(low_cardinality_key_string_two_level) \
    M(low_cardinality_key_fixed_string_two_level)

#define APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M) \
    M(time_bucket_key16_two_level) \
    M(time_bucket_key32_two_level) \
    M(time_bucket_key64_two_level) \
    M(time_bucket_keys32_two_level) \
    M(time_bucket_keys64_two_level) \
    M(time_bucket_keys128_two_level) \
    M(time_bucket_keys256_two_level) \
    M(time_bucket_nullable_keys128_two_level) \
    M(time_bucket_nullable_keys256_two_level) \
    M(time_bucket_low_cardinality_keys128_two_level) \
    M(time_bucket_low_cardinality_keys256_two_level) \
    M(time_bucket_serialized_two_level)

#define APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M) \
    APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M) \
    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:
                break;
            case Type::without_key:
                break;

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        NAME = std::make_unique<decltype(NAME)::element_type>(); \
        break;
                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        }

        type = type_;

        /// proton: start. Setup window key size since we will need use the size to extract the window key value
        /// and sort the window key in a sorted map for recycle
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        chassert(key_offset); \
        NAME->data.setBucketKeyBytesAndOffset(key_sizes[0], key_offset.value()); \
        break;
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

            default:
                break;
        }
        /// proton: ends;
    }

    /// clean up all in memory states and the corresponding arena pools used to hold these states
    void reset() override;
    void resetAndCreateAggregatesPool() { aggregates_pool = std::make_shared<Arena>(); }
    void resetAndCreateRetractPool() { retract_pool = std::make_shared<Arena>(); }

    /// Number of rows (different keys).
    size_t size() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return 0;
            case Type::without_key:
                return 1;

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return NAME->data.size() + (without_key != nullptr);
                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        }

        UNREACHABLE();
    }

    /// The size without taking into account the row in which data is written for the calculation of TOTALS.
    size_t sizeWithoutOverflowRow() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return 0;
            case Type::without_key:
                return 1;

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return NAME->data.size();
                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        }

        UNREACHABLE();
    }

    const char * getMethodName() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return "EMPTY";
            case Type::without_key:
                return "without_key";

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
        return #NAME;
                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        }

        UNREACHABLE();
    }

    bool isTwoLevel() const noexcept override { return isStaticBucketTwoLevel() || isTimeBucketTwoLevel(); }

    bool isStaticBucketTwoLevel() const noexcept
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        return true;
            APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
            default:
                return false;
        }
    }

    bool isTimeBucketTwoLevel() const noexcept
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        return true;
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
            default:
                return false;
        }
    }

#define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys32) \
    M(keys64) \
    M(keys128) \
    M(keys256) \
    M(serialized) \
    M(nullable_keys128) \
    M(nullable_keys256) \
    M(low_cardinality_key32) \
    M(low_cardinality_key64) \
    M(low_cardinality_keys128) \
    M(low_cardinality_keys256) \
    M(low_cardinality_key_string) \
    M(low_cardinality_key_fixed_string)

#define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
    M(key8) \
    M(key16) \
    M(keys16) \
    M(key64_hash64) \
    M(key_string_hash64) \
    M(key_fixed_string_hash64) \
    M(keys128_hash64) \
    M(keys256_hash64) \
    M(serialized_hash64) \
    M(low_cardinality_key8) \
    M(low_cardinality_key16)

#define APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M) \
    APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)

    bool isConvertibleToTwoLevel() const
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        return true;

            APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)

#undef M
            default:
                return false;
        }
    }

    void convertToTwoLevel();

/// proton: starts
#define APPLY_FOR_LOW_CARDINALITY_VARIANTS_STREAMING(M) \
    M(low_cardinality_key8) \
    M(low_cardinality_key16) \
    M(low_cardinality_key32) \
    M(low_cardinality_key64) \
    M(low_cardinality_keys128) \
    M(low_cardinality_keys256) \
    M(low_cardinality_key_string) \
    M(low_cardinality_key_fixed_string) \
    M(low_cardinality_key32_two_level) \
    M(low_cardinality_key64_two_level) \
    M(low_cardinality_keys128_two_level) \
    M(low_cardinality_keys256_two_level) \
    M(low_cardinality_key_string_two_level) \
    M(low_cardinality_key_fixed_string_two_level) \
    M(time_bucket_low_cardinality_keys128_two_level) \
    M(time_bucket_low_cardinality_keys256_two_level)

    /// proton ends
    bool isLowCardinality() const noexcept override
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: \
        return true;

            APPLY_FOR_LOW_CARDINALITY_VARIANTS_STREAMING(M)
#undef M
            default:
                return false;
        }
    }

    static HashMethodContextPtr createCache(Type type, const HashMethodContext::Settings & settings)
    {
        switch (type)
        {
            case Type::without_key:
                return nullptr;

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME: \
    { \
        using TPtr##NAME = decltype(MemoryAggregatedDataVariants::NAME); \
        using T##NAME = typename TPtr##NAME ::element_type; \
        return T##NAME ::State::createContext(settings); \
    }

                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M

            default:
                throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }

    ALWAYS_INLINE ArenaPtr getOrCreateTimeBucketArena(Int64 window_bucket)
    {
        chassert(isTimeBucketTwoLevel());

        auto iter = time_bucket_arenas.find(window_bucket);
        if (iter == time_bucket_arenas.end())
        {
            auto [new_iter, inserted] = time_bucket_arenas.emplace(window_bucket, std::make_shared<Arena>());
            assert(inserted);
            iter = new_iter;
        }

        return iter->second;
    }

    size_t remainingBucketArenas() const noexcept { return time_bucket_arenas.size(); }

    std::tuple<size_t, size_t> removeBucketsBefore(Int64 max_bucket);

    void updateMetrics(AggregatedDataMetrics & metrics, size_t total_size_of_aggregate_states) const override
    {
        switch (type)
        {
            case MemoryAggregatedDataVariants::Type::EMPTY:
                break;
            case MemoryAggregatedDataVariants::Type::without_key:
            {
                metrics.total_aggregated_rows += 1;
                metrics.total_bytes_of_aggregate_states += total_size_of_aggregate_states;
                break;
            }

#define M(NAME, IS_TWO_LEVEL) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        auto & table = NAME->data; \
        metrics.total_aggregated_rows += table.size(); \
        metrics.hash_buffer_bytes += table.getBufferSizeInBytes(); \
        metrics.hash_buffer_cells += table.getBufferSizeInCells(); \
        metrics.total_bytes_of_aggregate_states += table.size() * total_size_of_aggregate_states; \
        break; \
    }

                APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        }

        if (aggregates_pool)
            metrics.total_bytes_in_arena += aggregates_pool->size();

        if (retract_pool)
            metrics.total_bytes_in_arena += retract_pool->size();

        for (const auto & [_, arena] : time_bucket_arenas)
            metrics.total_bytes_in_arena += arena->size();
    }

    /// Existed versions:
    ///   STATE V3 - REVISION 14 (Add updates tracking state)
    ///   STATE V4 - REVISION 140 (Track updates for retract state)
    static constexpr VersionType version = 4;
};

using MemoryAggregatedDataVariantsPtr = std::shared_ptr<MemoryAggregatedDataVariants>;
using ManyMemoryAggregatedDataVariants = std::vector<MemoryAggregatedDataVariantsPtr>;
using ManyMemoryAggregatedDataVariantsPtr = std::shared_ptr<ManyMemoryAggregatedDataVariants>;

/// Get the aggregation variant by its type.
template <typename Method>
Method & getDataVariant(MemoryAggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL) \
    template <> \
    inline decltype(MemoryAggregatedDataVariants::NAME)::element_type & \
    getDataVariant<decltype(MemoryAggregatedDataVariants::NAME)::element_type>(MemoryAggregatedDataVariants & variants) \
    { \
        return *variants.NAME; \
    }

APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)

#undef M

}

}
