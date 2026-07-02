#pragma once

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <vector>

namespace cluster
{
/// usage : approximateSerializedSizeOf(std::string, std::vector<int32_t ... builtin numeric types etc>, ...)
/// T could be rewritten as concept which has `T::empty` and `T::size` etc methods
template <typename... T>
size_t approximateSerializedSizeOf(const T &... args)
{
    auto size_of = [](const auto & s) { return sizeof(s.size()) + (s.empty() ? 0 : s.size() * sizeof(*s.begin())); };
    return (size_of(args) + ...);
}

template <bool pointer, typename Container>
size_t totalApproximateSerializedSizeOf(const Container & c)
{
    size_t size = sizeof(c.size());
    for (const auto & e : c)
    {
        if constexpr (pointer)
            size += e->approximateSerializedSize();
        else
            size += e.approximateSerializedSize();
    }
    return size;
}

/// UUID string example : 9e8796be-e0af-476a-aee5-9f2b1c127d9b
inline bool isUUIDString(std::string_view s)
{
    if (s.size() >= 36)
        return (s[8] == '-') && (s[13] == '-') && (s[18] == '-') && (s[23] == '-');

    return false;
}

template <bool pointer, typename Container>
std::string stringOfStringables(const Container & c)
{
    std::vector<std::string> element_strings;
    element_strings.reserve(c.size());
    for (const auto & e : c)
    {
        if constexpr (pointer)
            element_strings.push_back(fmt::format("{{{}}}", e->string()));
        else
            element_strings.push_back(fmt::format("{{{}}}", e.string()));
    }

    return fmt::format("[{}]", fmt::join(element_strings, ", "));
}
}
