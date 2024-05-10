#include "ASTPauseMaterializedViewQuery.h"

#include <IO/Operators.h>

namespace DB
{
namespace Streaming
{
ASTPtr ASTPauseMaterializedViewQuery::clone() const
{
    auto res = std::make_shared<ASTPauseMaterializedViewQuery>();
    res->mvs = mvs->clone();
    res->children.push_back(res->mvs);
    return res;
}

void ASTPauseMaterializedViewQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "PAUSE MATERIALIZED VIEW ";
    if (mvs)
        mvs->formatImpl(settings, state, frame);
}
}
}
