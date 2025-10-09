#include <Parsers/ASTDropAlertQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTDropAlertQuery::clone() const
{
    auto cloned = std::make_shared<ASTDropAlertQuery>(*this);
    cloned->children.clear();

    cloned->name = name->clone();
    cloned->children.push_back(cloned->name);

    return cloned;
}

void ASTDropAlertQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP ALERT ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "") << (settings.hilite ? hilite_identifier : "");
    if (database)
        settings.ostr << backQuoteIfNeed(getDatabase()) << ".";

    settings.ostr << backQuoteIfNeed(getName()) << (settings.hilite ? hilite_none : "");
}

String ASTDropAlertQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTDropAlertQuery::getName() const
{
    String result;
    tryGetIdentifierNameInto(name, result);
    return result;
}

}
