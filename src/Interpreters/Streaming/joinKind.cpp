#include "joinKind.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
Kind toStreamingKind(ASTTableJoin::Kind kind)
{
    switch (kind)
    {
        case ASTTableJoin::Kind::Inner:
            return Kind::Inner;
        case ASTTableJoin::Kind::Left:
            return Kind::Left;
        /// FIXME, when we support right join uncomment this clause
        /// case ASTTableJoin::Kind::Right:
        ///    return Kind::Right;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports inner/left join");
    }
}

Strictness toStreamingStrictness(ASTTableJoin::Strictness strictness, bool is_range)
{
    switch (strictness)
    {
        case ASTTableJoin::Strictness::Any:
            if (is_range)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream join only supports any/all/asof/range join");
            else
                return Strictness::Any;
        case ASTTableJoin::Strictness::All:
            if (is_range)
                return Strictness::Range;
            else
                return Strictness::All;
        case ASTTableJoin::Strictness::Asof:
            if (is_range)
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
