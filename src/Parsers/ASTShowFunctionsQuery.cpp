#include <Parsers/ASTShowFunctionsQuery.h>

namespace DB
{
void ASTShowFunctionsQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked f) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW FUNCTIONS" << (settings.hilite ? hilite_none : "");
    if (where_expression)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(settings, state, f);
    }
}
}
