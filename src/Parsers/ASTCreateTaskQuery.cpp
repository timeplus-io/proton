#include <Parsers/ASTCreateTaskQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>

namespace DB
{
ASTPtr ASTCreateTaskQuery::clone() const
{
    auto res = std::make_shared<ASTCreateTaskQuery>(*this);
    res->children.clear();

    if (database)
    {
        res->database = database->clone();
        res->children.push_back(res->name);
    }

    res->name = name->clone();
    res->children.push_back(res->name);

    // res->set(res->select, select->clone());
    return res;
}

void ASTCreateTaskQuery::formatImpl(
    const IAST::FormatSettings & settings, IAST::FormatState & /*state*/, IAST::FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "TASK ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "");
    if (database)
        settings.ostr << backQuoteIfNeed(getDatabase()) << ".";

    settings.ostr << backQuoteIfNeed(getName()) << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nSCHEDULE " << (settings.hilite ? hilite_none : "");
    if (!cron.empty())
    {
        settings.ostr << cron;
    }
    else
    {
        settings.ostr << interval << interval_unit.toAbbreviation();
    }

    if (timeout > 0)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nTIMEOUT " << (settings.hilite ? hilite_none : "");
        settings.ostr << timeout << timeout_unit.toAbbreviation();
    }

    if (!checkpoint.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nCHECKPOINT " << (settings.hilite ? hilite_none : "");
        settings.ostr << "'" << checkpoint << "'";
    }

    if (to_table_id)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nINTO " << (settings.hilite ? hilite_none : "")
                      << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
                      << backQuoteIfNeed(to_table_id.table_name);
    }

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nAS " << (settings.hilite ? hilite_none : "");
    settings.ostr << "\n  " << sql;
}

String ASTCreateTaskQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTCreateTaskQuery::getName() const
{
    String result;
    tryGetIdentifierNameInto(name, result);
    return result;
}

}
