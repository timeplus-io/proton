#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Common/Nulls.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace cluster
{
template <typename ByteContainer, typename Serializable>
ByteContainer serialize(const Serializable & s, uint16_t version)
{
    ByteContainer buffer;

    if constexpr (std::is_same_v<ByteVector, cluster::ByteVector>)
        buffer.reserve(s.approximateSerializedSize());
    else
        /// For std::string, vector, we like to real allocate
        /// to void reallocate since we already have approx serialized size
        buffer.resize(s.approximateSerializedSize());

    DB::WriteBufferFromVector<ByteContainer> wb(buffer);
    s.serialize(wb, version);
    return buffer;
}

template <typename Serializable>
void deserialize(Serializable & s, std::string_view data, uint16_t version)
{
    DB::ReadBufferFromString rb(data);
    s.deserialize(rb, version);
}

template <typename ByteContainer, typename T>
ByteContainer serializeBinary(const T & t)
{
    ByteContainer buffer;

    if constexpr (std::is_same_v<ByteVector, cluster::ByteVector>)
        buffer.reserve(sizeof(t));
    else
        /// For std::string, vector, we like to real allocate
        /// to void reallocate since we already have approx serialized size
        buffer.resize(sizeof(t));

    DB::WriteBufferFromVector<ByteContainer> wb(buffer);
    DB::writeBinary(t, wb);
    return buffer;
}

template <typename T>
T deserializeBinary(std::string_view data)
{
    T t;
    DB::ReadBufferFromString rb(data);
    DB::readBinary(t, rb);
    return t;
}


template <typename Serializable, bool shared>
auto parse(DB::ReadBuffer & rb, uint16_t version)
{
    if constexpr (shared)
    {
        auto s = std::make_shared<Serializable>();
        s->deserialize(rb, version);
        return s;
    }
    else
    {
        Serializable s;
        s.deserialize(rb, version);
        return s;
    }
}

template <typename Serializable, bool shared>
auto parse(std::string_view data, uint16_t version)
{
    DB::ReadBufferFromString rb(data);
    return parse<Serializable, shared>(rb, version);
}

template <typename Serializable, bool shared>
auto parse(const ByteVector & data, uint16_t version)
{
    DB::ReadBufferFromMemory rb(data.data(), data.size());
    return parse<Serializable, shared>(rb, version);
}

template <typename Container>
void serialize(const Container & c, DB::WriteBuffer & wb)
{
    using E = typename Container::value_type;

    static_assert(std::is_integral_v<E> || std::is_same_v<E, std::string>, "Only support serializing integral or string container");

    DB::writeVarUInt(c.size(), wb);

    for (const auto & e : c)
    {
        if constexpr (std::is_unsigned_v<E>)
        {
            DB::writeVarUInt(e, wb);
        }
        else if constexpr (std::is_signed_v<E>)
        {
            DB::writeVarInt(e, wb);
        }
        else if constexpr (std::is_same_v<E, std::string>)
        {
            DB::writeStringBinary(e, wb);
        }
    }
}

template <typename Container>
void deserialize(Container & c, DB::ReadBuffer & rb)
{
    /// Removing const, & and volatile
    using E = typename Container::value_type;

    static_assert(std::is_integral_v<E> || std::is_same_v<E, std::string>, "Only support deserializing integral or string container");

    size_t size = 0;
    DB::readVarUInt(size, rb);

    if constexpr (std::is_integral_v<E>)
        c.resize(size);
    else if constexpr (std::is_same_v<E, std::string>)
        c.reserve(size);

    for (size_t n = 0; n < size; ++n)
    {
        if constexpr (std::is_unsigned_v<E>)
        {
            DB::readVarUInt(c[n], rb);
        }
        else if constexpr (std::is_signed_v<E>)
        {
            DB::readVarInt(c[n], rb);
        }
        else if constexpr (std::is_same_v<E, std::string>)
        {
            E s;
            DB::readStringBinary(s, rb);
            c.emplace_back(std::move(s));
        }
    }
}

template <typename T>
void serializeEnum(T v, DB::WriteBuffer & wb)
{
    static_assert(sizeof(T) == 1, "Shall use this for one byte enum class");

    DB::writeIntBinary(std::to_underlying(v), wb);
}

template <typename T>
T deserializeEnum(DB::ReadBuffer & rb)
{
    static_assert(sizeof(T) == 1, "Shall use this for one byte enum class");

    std::underlying_type_t<T> v = 0;
    DB::readIntBinary(v, rb);
    return static_cast<T>(v);
}

template <typename T>
void serializeEnumWide(T v, DB::WriteBuffer & wb)
{
    static_assert(sizeof(T) == 2, "Shall use this for two bytes enum class");

    DB::writeVarUInt(std::to_underlying(v), wb);
}

template <typename T>
T deserializeEnumWide(DB::ReadBuffer & rb)
{
    static_assert(sizeof(T) == 2, "Shall use this for two bytes enum class");

    std::underlying_type_t<T> v = 0;
    DB::readVarUInt(v, rb);
    return static_cast<T>(v);
}


template <bool pointer, typename Container>
void serializeSerializables(const Container & c, DB::WriteBuffer & wb, uint16_t version)
{
    DB::writeVarUInt(c.size(), wb);
    for (const auto & e : c)
    {
        if constexpr (pointer)
            e->serialize(wb, version);
        else
            e.serialize(wb, version);
    }
}

template <bool shared, typename Container>
void deserializeSerializables(Container & c, DB::ReadBuffer & rb, uint16_t version)
{
    uint64_t size = 0;
    DB::readVarUInt(size, rb);
    c.reserve(size);

    if constexpr (shared)
    {
        using E = typename Container::value_type::element_type;
        for (uint64_t i = 0; i < size; ++i)
            c.push_back(cluster::parse<E, shared>(rb, version));
    }
    else
    {
        using E = typename Container::value_type;
        for (uint64_t i = 0; i < size; ++i)
            c.push_back(cluster::parse<E, shared>(rb, version));
    }
}

}
