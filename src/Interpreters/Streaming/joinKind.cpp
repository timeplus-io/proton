#include <Interpreters/Streaming/joinKind.h>

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
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports inner/left join");
    }
}

Strictness toJoinStrictness(JoinStrictness strictness, bool is_range_join)
{
    switch (strictness)
    {
        case JoinStrictness::Any:
            if (is_range_join)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports any/all/asof/range join");
            else
                return Strictness::Any;
        case JoinStrictness::All:
            if (is_range_join)
                return Strictness::Range;
            else
                return Strictness::All;
        case JoinStrictness::Asof:
            if (is_range_join)
                /// FIXME, when we support range asof, revise this
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports any/all/asof/range join");
                /// return Strictness::RangeAsof;
            else
                return Strictness::Asof;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports any/all/asof/range join");
    }
}
}
}
