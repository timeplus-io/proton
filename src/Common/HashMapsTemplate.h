#pragma once

#include <base/defines.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;


template <typename Map, typename MappedSerializer>
void serializeHashMap(const Map & map, MappedSerializer && mapped_serializer, WriteBuffer & wb)
{
    /// Serialization layout
    /// [uint32(size)] [<key><mapped>] ...
    DB::writeIntBinary<UInt32>(static_cast<UInt32>(map.size()), wb);
    const_cast<Map &>(map).forEachValue([&](const auto & key, const auto & mapped) {
        DB::writeBinary(key, wb);
        mapped_serializer(mapped, wb);
    });
}

/// For StringHashMap or TwoLevelStringHashMap, it requires StringRef key padded 8 keys or zero terminated.
/// If the key is ColumnString, the `@template_param: requires_zero_terminated_string` is true
template <bool requires_zero_terminated_string, typename Map, typename MappedDeserializer>
void deserializeHashMap(Map & map, MappedDeserializer && mapped_deserializer, Arena & pool, ReadBuffer & rb)
{
    typename Map::key_type key;
    typename Map::LookupResult lookup_result;
    bool inserted;
    UInt32 size;
    DB::readIntBinary<UInt32>(size, rb);
    for (size_t i = 0; i < size; ++i)
    {
        /* Key */
        if constexpr (std::is_same_v<typename Map::key_type, StringRef>)
        {
            if constexpr (requires_zero_terminated_string)
                key = DB::readStringBinaryWithZerotTerminatedInto(pool, rb);
            else
                key = DB::readStringBinaryInto(pool, rb);

            map.emplace(SerializedKeyHolder{key, pool}, lookup_result, inserted);
        }
        else
        {
            DB::readBinary(key, rb);
            map.emplace(key, lookup_result, inserted);
        }
        assert(inserted);
        /* Mapped */
        mapped_deserializer(lookup_result->getMapped(), pool, rb);
    }
}

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

    template <typename MappedSerializer>
    void serialize(MappedSerializer && mapped_serializer, WriteBuffer & wb) const
    {
        switch (type)
        {
#define M(NAME) \
    case HashType::NAME: { \
        assert(NAME); \
        serializeHashMap(*NAME, mapped_serializer, wb); \
        return; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        UNREACHABLE();
    }

    template <typename MappedDeserializer>
    void deserialize(MappedDeserializer && mapped_deserializer, Arena & pool, ReadBuffer & rb)
    {
        switch (type)
        {
#define M(NAME) \
    case HashType::NAME: { \
        assert(NAME); \
        deserializeHashMap<false>(*NAME, mapped_deserializer, pool, rb); \
        return; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
            UNREACHABLE();
        }
    }

    HashType type;
};

template <typename Mapped>
using FindResultImpl = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

/// Dummy key getter, always find nothing, used for JOIN ON NULL
template <typename Mapped>
class KeyGetterEmpty
{
public:
    struct MappedType
    {
        using mapped_type = Mapped;
    };

    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped>;

    KeyGetterEmpty() = default;

    FindResult findKey(MappedType, size_t, const Arena &) { return FindResult(); }
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
