#pragma once

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HybridHashTable/HybridHashTable.h>
#include <Common/HybridHashTable/HybridHashType.h>
#include <Common/HybridHashTable/TimeBucketHybridHashTable.h>
#include <Common/serde.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
}

struct HybridHashTableTemplate
{
    /// Single key
    std::unique_ptr<HybridHashTable<uint8_t>> key8;
    std::unique_ptr<HybridHashTable<uint16_t>> key16;
    std::unique_ptr<HybridHashTable<uint32_t>> key32;
    std::unique_ptr<HybridHashTable<uint64_t>> key64;
    std::unique_ptr<HybridHashTable<String>> key_fixed_string;
    std::unique_ptr<HybridHashTable<String>> key_string;

    /// Multiple fixed keys
    std::unique_ptr<HybridHashTable<uint16_t>> keys16;
    std::unique_ptr<HybridHashTable<uint32_t>> keys32;
    std::unique_ptr<HybridHashTable<uint64_t>> keys64;
    std::unique_ptr<HybridHashTable<UInt128>> keys128;
    std::unique_ptr<HybridHashTable<UInt256>> keys256;

    /// Multiple (variant length) keys or other data types which are beyond 256bits
    std::unique_ptr<HybridHashTable<String>> key_serialized;

    /// Multiple (variant length) keys or other data types which are beyond 256bits for join
    std::unique_ptr<HybridHashTable<UInt128, UInt128TrivialHash>> key_hashed;

    /// Two level time bucket
    std::unique_ptr<TimeBucketHybridHashTable<uint16_t>> time_bucket_key16_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<uint32_t>> time_bucket_key32_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<uint64_t>> time_bucket_key64_two_level;

    std::unique_ptr<TimeBucketHybridHashTable<uint32_t>> time_bucket_keys32_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<uint64_t>> time_bucket_keys64_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<UInt128>> time_bucket_keys128_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<UInt256>> time_bucket_keys256_two_level;
    std::unique_ptr<TimeBucketHybridHashTable<String>> time_bucket_key_serialized_two_level;

private:
    HybridHashType type = HybridHashType::Empty;

public:
    void setWithoutKeyType() noexcept { type = HybridHashType::WithoutKey; }
    bool isWithoutKeyType() const noexcept { return type == HybridHashType::WithoutKey; }
    auto getType() const noexcept { return type; }

    bool empty() const noexcept { return type == HybridHashType::Empty; }

    bool isLowCardinality() const noexcept
    {
        /// FIXME, support low cardinality
        return false;
    }

    bool isTwoLevel() const noexcept
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return true;
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M
            default:
                return false;
        }
    }

    void init(
        HybridHashType type_,
        HybridHashTableConfig config,
        const std::vector<size_t> & key_sizes,
        LoggerPtr logger,
        std::optional<size_t> bucket_key_offset = {})
    {
        switch (type_)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                break;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        using HHT = typename decltype(NAME)::element_type; \
        auto ser = [](const HHT::KeyType & key, WriteBuffer & wb) { \
            writeBinary(key, wb); \
            return DB::ErrorCodes::OK; \
        }; \
        auto des = [](HHT::KeyType & key, ReadBuffer & rb) { \
            readBinary(key, rb); \
            return DB::ErrorCodes::OK; \
        }; \
        NAME = std::make_unique<HHT>(std::move(config), std::move(ser), std::move(des), logger); \
        break; \
    }
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }

        switch (type_)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(bucket_key_offset.has_value()); \
        NAME->setBucketKeyBytesAndOffset(key_sizes[0], bucket_key_offset.value()); \
        break;
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M
            default:
                break;
        }

        type = type_;
    }

    void reset()
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                break;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        NAME.reset(); \
        break;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }

        type = HybridHashType::Empty;
    }

    void clear()
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            NAME->clear(); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    const HybridHashTableMetrics & metrics() const noexcept
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return empty_metrics;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        return NAME ? NAME->getMetrics() : empty_metrics;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    size_t approximateCount() const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                return 0;
            case HybridHashType::WithoutKey:
                return 1;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->approximateCount() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    size_t getBufferSizeInBytes() const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                return 0;
            case HybridHashType::WithoutKey:
                return 0;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    size_t getBufferSizeInCells() const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                return 0;
            case HybridHashType::WithoutKey:
                return 0;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    void serialize(WriteBuffer & wb) const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            NAME->write(wb); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    void deserialize(ReadBuffer & rb, const HybridHashTableConfig::ValueDeserializer & old_deserializer = {})
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            NAME->read(rb, old_deserializer); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    void flush()
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            return NAME->flush(); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    void reload(const HybridHashTableConfig::ValueDeserializer & old_deserializer = {})
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                return;
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            return NAME->reload(old_deserializer); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
        }
    }

    std::vector<Int64> buckets()
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            return NAME->buckets(); \
        return {};
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M
            default:
                return {};
        }
    }

    void reinitBuckets(const std::vector<Int64> & buckets)
    {
        switch (type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            return NAME->reinitBuckets(buckets); \
        return;
            APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M
            default:
                return;
        }
    }

    HybridHashTableMetrics empty_metrics;
};


template <typename T>
struct HybridHashTableTemplateTagged
{
    using TaggedType = T;

    HybridHashTableTemplate table;
};

}
