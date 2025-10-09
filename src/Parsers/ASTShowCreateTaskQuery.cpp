#include <Parsers/ASTShowCreateTaskQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>


namespace DB
{
String ASTShowCreateTaskQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTShowCreateTaskQuery::getName() const
{
    String result;
    tryGetIdentifierNameInto(name, result);
    return result;
}

ASTPtr ASTShowCreateTaskQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateTaskQuery>(*this);
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

void ASTShowCreateTaskQuery::formatQueryImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE TASK" << " " << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "");
    if (database)
        settings.ostr << backQuoteIfNeed(getDatabase()) << ".";

    settings.ostr << getName() << (settings.hilite ? hilite_none : "");
}
}

