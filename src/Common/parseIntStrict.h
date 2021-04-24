#pragma once

#include "Exception.h"

#include <common/types.h>

#include <charconv>


/// Daisy : starts

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}

template <typename Integer>
Integer parseIntStrict(const String & s, String::size_type lpos, String::size_type rpos)
{
    if (rpos <= lpos || rpos > s.size())
    {
        throw Exception("Invalid number " + String{s, lpos, rpos - lpos}, ErrorCodes::INVALID_SETTING_VALUE);
    }

    Integer n = 0;
    auto [p, ec] = std::from_chars(&s[lpos], &s[rpos], n);
    if (ec != std::errc())
    {
        throw Exception("Invalid number " + String{s, lpos, rpos - lpos}, ErrorCodes::INVALID_SETTING_VALUE);
    }
    else if (p != &s[rpos])
    {
        throw Exception("Invalid number " + String{s, lpos, rpos - lpos}, ErrorCodes::INVALID_SETTING_VALUE);
    }
    return n;
}
}

/// Daisy : ends
