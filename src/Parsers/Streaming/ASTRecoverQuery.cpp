#include "ASTRecoverQuery.h"

#include <Parsers/ASTLiteral.h>

namespace DB
{
namespace Streaming
{

ASTPtr ASTRecoverQuery::clone() const
{
    auto res = std::make_shared<ASTRecoverQuery>(*this);

    if (query_id)
        res->query_id = query_id->clone();

    return res;
}

void ASTRecoverQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "RECOVER FROM ";
    if (query_id)
        query_id->formatImpl(settings, state, frame);
}
}
}
