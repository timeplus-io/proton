#pragma once

#include <Core/Field.h>
#include <base/StringRef.h>
#include <base/types.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>

#include <any>

namespace DB::Streaming::Substream
{
enum class GroupByKeys : uint8_t
{
    PARTITION_KEYS,
    WINDOWED_PARTITION_KEYS, /// KEYS: <window_start/window_end> + <substream keys>
    SINGLE_KEY
};

/// -> (u)int16/32/64/128/256, double, StringRef, etc.)
struct ID
{
    std::variant<Field, StringRef> key{};
    size_t saved_hash{0};
    ArenaPtr pool{}; /// Keep cache persisted id
    bool materialized{true};

    ID() = default;
    ID(const std::variant<Field, StringRef> & key_, size_t saved_hash_, ArenaPtr pool_, bool materialized_)
        : key(key_), saved_hash(saved_hash_), pool(pool_), materialized(materialized_)
    {
    }

    bool empty() const { return key.index() == std::variant_npos; }
    operator bool() const { return !empty(); }
    bool operator==(const ID & rhs) const { return saved_hash == rhs.saved_hash && key == rhs.key; }

    ID & materialize()
    {
        if (!materialized)
        {
            materialized = true;
            auto & str_ref = std::get<StringRef>(key);
            assert(str_ref.size > 0);
            key = StringRef(pool->insert(str_ref.data, str_ref.size), str_ref.size);
        }
        return *this;
    }

    const ID materialize() const
    {
        ID id{*this};
        return id.materialize();
    }
};
using IDs = std::vector<ID>;
}

/// Common hash / equal_to ...
namespace std
{
template <>
struct hash<DB::Streaming::Substream::ID>
{
    std::size_t operator()(const auto & s) const noexcept { return s.saved_hash; }
};
template <>
struct equal_to<DB::Streaming::Substream::ID>
{
    bool operator()(auto && x, auto && y) const noexcept
    {
        auto index = x.key.index();
        if (likely(index == y.key.index()))
        {
            if (index == 0)
                return std::get<DB::Field>(x.key) == std::get<DB::Field>(y.key);
            else if (index == 1)
                /// We skip compare hash since it should be already used in hash
                return /* x.saved_hash == y.saved_hash && */ std::get<StringRef>(x.key) == std::get<StringRef>(y.key);
            else
            {
                assert(index == std::variant_npos);
                return true; /// No key
            }
        }

        return false;
    }
};
}

template <>
struct fmt::formatter<DB::Streaming::Substream::ID>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        auto it = ctx.begin();
        auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Streaming::Substream::ID & x, FormatContext & ctx)
    {
        if (auto * field = std::get_if<DB::Field>(&x.key))
            return format_to(ctx.out(), "{}(hash:{})", toString(*field), x.saved_hash);
        else if (auto * str = std::get_if<StringRef>(&x.key))
            return format_to(ctx.out(), "{}(hash:{})", *str, x.saved_hash);
        else
            return format_to(ctx.out(), "(empty substream id)");
    }
};
