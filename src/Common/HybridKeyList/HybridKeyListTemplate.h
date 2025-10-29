#pragma once

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HybridHashTable/HybridHashType.h>
#include <Common/HybridKeyList/HybridKeyList.h>
#include <Common/serde.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int UNSUPPORTED;
}

struct HybridKeyListTemplate
{
    /// Single key
    std::unique_ptr<HybridKeyList<uint8_t>> key8;
    std::unique_ptr<HybridKeyList<uint16_t>> key16;
    std::unique_ptr<HybridKeyList<uint32_t>> key32;
    std::unique_ptr<HybridKeyList<uint64_t>> key64;
    std::unique_ptr<HybridKeyList<String>> key_fixed_string;
    std::unique_ptr<HybridKeyList<String>> key_string;

    /// Multiple fixed keys
    std::unique_ptr<HybridKeyList<uint16_t>> keys16;
    std::unique_ptr<HybridKeyList<uint32_t>> keys32;
    std::unique_ptr<HybridKeyList<uint64_t>> keys64;
    std::unique_ptr<HybridKeyList<UInt128>> keys128;
    std::unique_ptr<HybridKeyList<UInt256>> keys256;

    /// Multiple (variant length) keys or other data types which are beyond 256bits
    std::unique_ptr<HybridKeyList<String>> key_serialized;

    /// Multiple (variant length) keys or other data types which are beyond 256bits for join
    std::unique_ptr<HybridKeyList<UInt128>> key_hashed;

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

    bool isTwoLevel() const noexcept { return false; }

    void init(HybridHashType type_, HybridKeyListConfig config, [[maybe_unused]] const std::vector<size_t> & key_sizes, LoggerPtr logger)
    {
        switch (type_)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            default:
                throw Exception(ErrorCodes::UNSUPPORTED, "Hybrid key list doesn't support hash_type={}", type_);

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
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
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
                [[fallthrough]];
            default:
                break;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        NAME.reset(); \
        break;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
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
                [[fallthrough]];
            default:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            NAME->clear(); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
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
            default:
                return 0;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->approximateCount() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
        }
    }

    size_t getBufferSizeInBytes() const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            default:
                return 0;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
        }
    }

    size_t getBufferSizeInCells() const
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            default:
                return 0;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
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
                [[fallthrough]];
            default:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            NAME->write(wb); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
        }
    }

    void deserialize(ReadBuffer & rb)
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            default:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        chassert(NAME); \
        if (NAME) \
            NAME->read(rb); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
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
                [[fallthrough]];
            default:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            NAME->flush(); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
        }
    }

    void reload()
    {
        switch (type)
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            default:
                return;

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        if (NAME) \
            return NAME->reload(); \
        return;
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
        }
    }
};


template <typename T>
struct HybridKeyListTemplateTagged
{
    using TaggedType = T;

    HybridKeyListTemplate key_list;
};

}
