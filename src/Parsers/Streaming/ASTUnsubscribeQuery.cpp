#include "ASTUnsubscribeQuery.h"

namespace DB
{
namespace Streaming
{

ASTPtr ASTUnsubscribeQuery::clone() const
{
    auto res = std::make_shared<ASTUnsubscribeQuery>(*this);

    if (query_id)
        res->query_id = query_id->clone();

    return res;
}

void ASTUnsubscribeQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "UNSUBSCRIBE TO ";
    if (query_id)
        query_id->formatImpl(settings, state, frame);
}
}
}
