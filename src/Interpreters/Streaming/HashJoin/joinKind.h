#pragma once

#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB::Streaming
{
/// Allows more optimal JOIN for typical cases.
enum class Strictness : uint8_t
{
    Latest = 0, /// Semi Join with any value from filtering table. For LEFT JOIN with Any and RightAny are the same.
    All, /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
    Asof, /// For the last JOIN column, pick the latest value
    Range,
    Multiple, /// Non unique join. Used for partial primary key join
    Anti, /// Emit left rows whose key is NOT present in the latest right snapshot
};

/// Join method.
enum class Kind : uint8_t
{
    Inner = 0, /// Leave only rows that was JOINed.
    Left, /// If in "right" table there is no corresponding rows, use default values instead.
    Right,
    Full,
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
        case Kind::Full:
            return Kind::Full;
    }

    UNREACHABLE();
}

Kind toJoinKind(JoinKind kind);
Strictness toJoinStrictness(JoinStrictness strictness, bool is_range_join);

}

