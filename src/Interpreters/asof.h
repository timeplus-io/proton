#pragma once
#include <string>

namespace DB
{
namespace ASOF
{

enum class Inequality
{
    None = 0,
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
    /// proton : starts
    RangeBetween,
    /// proton : ends
};

inline Inequality getInequality(const std::string & func_name)
{
    Inequality inequality{Inequality::None};
    if (func_name == "less")
        inequality = Inequality::Less;
    else if (func_name == "greater")
        inequality = Inequality::Greater;
    else if (func_name == "less_or_equals")
        inequality = Inequality::LessOrEquals;
    else if (func_name == "greater_or_equals")
        inequality = Inequality::GreaterOrEquals;
    else if (func_name == "date_diff_within")
        inequality = Inequality::RangeBetween;
    return inequality;
}

inline Inequality reverseInequality(Inequality inequality)
{
    if (inequality == Inequality::Less)
        return Inequality::Greater;
    else if (inequality == Inequality::Greater)
        return Inequality::Less;
    else if (inequality == Inequality::LessOrEquals)
        return Inequality::GreaterOrEquals;
    else if (inequality == Inequality::GreaterOrEquals)
        return Inequality::LessOrEquals;
    else if (inequality == Inequality::RangeBetween)
        return Inequality::RangeBetween;
    return Inequality::None;
}

}
}
