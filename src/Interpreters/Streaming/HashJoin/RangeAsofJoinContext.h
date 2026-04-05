#pragma once

#include <Core/Joins.h>
#include <base/types.h>

#include <fmt/format.h>
#include <magic_enum.hpp>
namespace DB
{
class WriteBuffer;
class ReadBuffer;

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
    ASOFJoinInequality left_inequality = ASOFJoinInequality::GreaterOrEquals;
    ASOFJoinInequality right_inequality = ASOFJoinInequality::LessOrEquals;

    /// For interval range, it is always in second
    Int64 lower_bound = 0;
    Int64 upper_bound = 0;

    /// Max number of range buckets to keep per stream. When the bucket count exceeds this limit,
    /// the oldest buckets are reclaimed regardless of watermark position.
    /// 0 means no limit (watermark-only reclamation).
    UInt64 max_buckets = 0;

    RangeType type = RangeType::None;

    void validate(Int64 max_range) const;

    String string() const
    {
        return fmt::format(
            "lower_bound={} upper_bound={} left_inequality={} right_inequality={} type={} max_buckets={}",
            lower_bound,
            upper_bound,
            magic_enum::enum_name(left_inequality),
            magic_enum::enum_name(right_inequality),
            magic_enum::enum_name(type),
            max_buckets);
    }

    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);
};
}
}
