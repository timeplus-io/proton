#pragma once

#include "Exception.h"

#include <base/types.h>

#include <charconv>


/// proton: starts

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_INTEGER_STRING;
}

template <typename Str, typename Integer>
Integer parseIntStrict(const Str & s, String::size_type lpos, String::size_type rpos)
{
    if (rpos <= lpos || rpos > s.size())
        throw Exception(ErrorCodes::INVALID_INTEGER_STRING, "Invalid params s={} lpos={} rpos={} size={}", s, lpos, rpos, s.size());

    Integer n = 0;
    /// Note for string_view, indexing beyond the very end throws exception
    auto [p, ec] = std::from_chars(&s[lpos], &s[rpos], n);
    if (ec != std::errc())
        throw Exception(ErrorCodes::INVALID_INTEGER_STRING, "Invalid number '{}' string, lpos={} rpos={} size={}", s, lpos, rpos, s.size());
    else if (p != &s[rpos])
        throw Exception(ErrorCodes::INVALID_INTEGER_STRING, "Invalid number '{}' string, only parse partial of it, lpos={} rpos={} size={}", s, lpos, rpos, s.size());

    return n;
}

template <typename Integer>
Integer parseIntStrict(const std::string & s, String::size_type lpos, String::size_type rpos)
{
    return parseIntStrict<std::string, Integer>(s, lpos, rpos);
}

template <typename Integer>
Integer parseIntStrict(const std::string_view & s, String::size_type lpos, String::size_type rpos)
{
    return parseIntStrict<std::string_view, Integer>(s, lpos, rpos);
}
}

/// proton: ends
