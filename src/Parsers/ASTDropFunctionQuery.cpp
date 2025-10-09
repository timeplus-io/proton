#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

/// proton: starts
ASTPtr ASTDropFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTDropFunctionQuery>(*this);
    res->children.clear();

    if (settings_ast)
    {
        res->settings_ast = settings_ast->clone();
        res->children.push_back(res->settings_ast);
    }

    return res;
}
/// proton: ends

void ASTDropFunctionQuery::formatQueryImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP FUNCTION ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(function_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
