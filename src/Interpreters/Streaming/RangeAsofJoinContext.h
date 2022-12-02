#pragma once

#include <Interpreters/asof.h>
#include <base/types.h>

#include <fmt/format.h>
#include <magic_enum.hpp>
namespace DB
{
namespace Streaming
{
enum class RangeType : uint8_t
{
    None,
    Interval,
    Integer,
};

struct RangeAsofJoinContext
{
    /// Left means range left
    ASOF::Inequality left_inequality = ASOF::Inequality::GreaterOrEquals;
    ASOF::Inequality right_inequality = ASOF::Inequality::LessOrEquals;

    /// For interval range, it is always in second
    Int64 lower_bound = 0;
    Int64 upper_bound = 0;

    RangeType type = RangeType::None;

    void validate(Int64 max_range) const;

    String string() const
    {
        return fmt::format(
            "lower_bound={} upper_bound={} left_inequality={} right_inequality={} type={}",
            lower_bound,
            upper_bound,
            magic_enum::enum_name(left_inequality),
            magic_enum::enum_name(right_inequality),
            magic_enum::enum_name(type));
    }
};
}
}
