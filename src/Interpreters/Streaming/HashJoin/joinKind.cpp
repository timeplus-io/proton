#include <Interpreters/Streaming/HashJoin/joinKind.h>
#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
Kind toJoinKind(JoinKind kind)
{
    switch (kind)
    {
        case JoinKind::Inner:
            return Kind::Inner;
        case JoinKind::Left:
            return Kind::Left;
        case JoinKind::Right:
            return Kind::Right;
        case JoinKind::Full:
            return Kind::Full;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported stream join kind: kind={}", magic_enum::enum_name(kind));
    }
}

Strictness toJoinStrictness(JoinStrictness strictness, bool is_range_join)
{
    if (is_range_join)
    {
        if (strictness != JoinStrictness::All)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream {} join doesn't support range join", strictness);

        return Strictness::Range;
    }

    switch (strictness)
    {
        case JoinStrictness::Any:
            return Strictness::Latest;
        case JoinStrictness::All:
            return Strictness::All;
        case JoinStrictness::Asof:
            return Strictness::Asof;
        case JoinStrictness::Anti:
            return Strictness::Anti;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports latest/all/asof/anti join");
    }
}
}
}
