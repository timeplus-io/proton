#include "ASTUnpauseMaterializedViewQuery.h"

#include <IO/Operators.h>

namespace DB
{
namespace Streaming
{

ASTPtr ASTUnpauseMaterializedViewQuery::clone() const
{
    auto res = std::make_shared<ASTUnpauseMaterializedViewQuery>();
    res->mvs = mvs->clone();
    res->children.push_back(res->mvs);
    return res;
}

void ASTUnpauseMaterializedViewQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "UNPAUSE MATERIALIZED VIEW ";
    if (mvs)
        mvs->formatImpl(settings, state, frame);
}
}
}
