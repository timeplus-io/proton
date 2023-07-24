#pragma once

#include <base/defines.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
/// HashMapsTemplate is a taken from HashJoin class and make it standalone
/// and could be shared among different components

/// Different types of keys for maps.
#define APPLY_FOR_HASH_KEY_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys128) \
    M(keys256) \
    M(hashed)

enum class HashType
{
#define M(NAME) NAME,
    APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
};

template <typename Mapped>
struct HashMapsTemplate
{
    using MappedType = Mapped;
    std::unique_ptr<FixedHashMap<UInt8, Mapped>> key8;
    std::unique_ptr<FixedHashMap<UInt16, Mapped>> key16;
    std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>> key32;
    std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>> key64;
    std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_string;
    std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_fixed_string;
    std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>> keys128;
    std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>> keys256;
    std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>> hashed;

    void create(HashType which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashType::NAME: \
        NAME = std::make_unique<typename decltype(NAME)::element_type>(); \
        break;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        type = which;
    }

    size_t getTotalRowCount() const
    {
        switch (type)
        {
#define M(NAME) \
    case HashType::NAME: \
        return NAME ? NAME->size() : 0;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        UNREACHABLE();
    }

    size_t getTotalByteCountImpl() const
    {
        switch (type)
        {
#define M(NAME) \
    case HashType::NAME: \
        return NAME ? NAME->getBufferSizeInBytes() : 0;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        UNREACHABLE();
    }

    size_t getBufferSizeInCells() const
    {
        switch (type)
        {
#define M(NAME) \
    case HashType::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        UNREACHABLE();
    }

    HashType type;
};

template <HashType type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, true>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashType::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, true>;
};

template <HashType type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};
}
