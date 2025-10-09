#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowCreateAlertQuery.h>
#include <Common/quoteString.h>

namespace DB
{
String ASTShowCreateAlertQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTShowCreateAlertQuery::getName() const
{
    String result;
    tryGetIdentifierNameInto(name, result);
    return result;
}

ASTPtr ASTShowCreateAlertQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateAlertQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    if (database)
    {
        res->database = database->clone();
        res->children.push_back(res->database);
    }
    res->name = name->clone();
    res->children.push_back(res->name);
    return res;
}

void ASTShowCreateAlertQuery::formatQueryImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE ALERT" << " " << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "");
    if (database)
        settings.ostr << backQuoteIfNeed(getDatabase()) << ".";

    settings.ostr << getName() << (settings.hilite ? hilite_none : "");
}
}
