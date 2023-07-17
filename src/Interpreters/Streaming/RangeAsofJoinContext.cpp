#include "RangeAsofJoinContext.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
}

namespace Streaming
{
void Streaming::RangeAsofJoinContext::validate(Int64 max_range) const
{
   if (type == RangeType::None)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Range join is not correctly setup");

    /// date_diff('second', left.column, right.column) between 0 and 10 instead of date_diff('second', left.column, right.column) between 1 and 11
    /// for latter case, user can rewrite : date_diff('second', data_sub(left.column, 1s), right.column) between 0 and 10
    if (lower_bound > 0)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Range join requires left bound of the range is less than or equal to zero, but got lower_bound={}",
            lower_bound);

    if (upper_bound < 0)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Range join requires right bound of the range is greater than or equal to zero, but got upper_bound={}",
            upper_bound);

    if (lower_bound >= upper_bound)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Range join requires upper bound of the range is bigger than the lower bound, but got lower_bound={}, upper_bound={}",
            lower_bound,
            upper_bound);

    if (upper_bound - lower_bound > max_range)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "The join range ({}, {}) is too big. If two streams have big difference in timestamps. Consider adjust the timestamp of one "
            "of the stream by doing timestamp subtraction / addition and then do join. For example: ... AND date_diff_within(2m, "
            "left_time_col, date_add(right_time_col, 2h))",
            lower_bound, upper_bound);

    if (left_inequality != ASOFJoinInequality::Greater && left_inequality != ASOFJoinInequality::GreaterOrEquals)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Range join requires lower bound of inequality shall be '>' or '>='");

    if (right_inequality != ASOFJoinInequality::Less && right_inequality != ASOFJoinInequality::LessOrEquals)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Range join requires upper bound of inequality shall be '<' or '<='");
}

void RangeAsofJoinContext::serialize(WriteBuffer & wb) const
{
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(left_inequality), wb);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(right_inequality), wb);
    DB::writeIntBinary(lower_bound, wb);
    DB::writeIntBinary(upper_bound, wb);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(type), wb);
}

void RangeAsofJoinContext::deserialize(ReadBuffer & rb)
{
    UInt16 recovered_left_inequality;
    DB::readIntBinary<UInt16>(recovered_left_inequality, rb);
    left_inequality = static_cast<ASOFJoinInequality>(recovered_left_inequality);

    UInt16 recovered_right_inequality;
    DB::readIntBinary<UInt16>(recovered_right_inequality, rb);
    right_inequality = static_cast<ASOFJoinInequality>(recovered_right_inequality);

    DB::readIntBinary(lower_bound, rb);
    DB::readIntBinary(upper_bound, rb);

    UInt16 recovered_type;
    DB::readIntBinary<UInt16>(recovered_type, rb);
    type = static_cast<RangeType>(recovered_type);
}

}
}
