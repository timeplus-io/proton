#pragma once

#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace Streaming
{
/// Allows more optimal JOIN for typical cases.
enum class Strictness
{
    Latest, /// Semi Join with any value from filtering table. For LEFT JOIN with Any and RightAny are the same.
    All, /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
    Asof, /// For the last JOIN column, pick the latest value
    Range,
    Multiple, /// Non unique join. Used for partial primary key join
};

/// Join method.
enum class Kind
{
    Inner, /// Leave only rows that was JOINed.
    Left, /// If in "right" table there is no corresponding rows, use default values instead.
    Right,
};

constexpr Kind flipKind(Kind lhs)
{
    switch (lhs)
    {
        case Kind::Inner:
            return lhs;
        case Kind::Left:
            return Kind::Right;
        case Kind::Right:
            return Kind::Left;
    }

    UNREACHABLE();
}

Kind toJoinKind(JoinKind kind);
Strictness toJoinStrictness(JoinStrictness strictness, bool is_range_join);
}
}
