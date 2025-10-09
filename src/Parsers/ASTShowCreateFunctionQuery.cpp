#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowCreateFunctionQuery.h>
#include <Common/quoteString.h>

namespace DB
{
String ASTShowCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}
void ASTShowCreateFunctionQuery::formatQueryImpl(
    const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE FUNCTION"
                  << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << backQuoteIfNeed(getFunctionName());
}
}
