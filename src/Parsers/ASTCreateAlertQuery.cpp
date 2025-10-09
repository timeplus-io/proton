#include <IO/Operators.h>
#include <Parsers/ASTCreateAlertQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>

namespace DB
{
ASTPtr ASTCreateAlertQuery::clone() const
{
    auto res = std::make_shared<ASTCreateAlertQuery>(*this);
    res->children.clear();

    if (database)
    {
        res->database = database->clone();
        res->children.push_back(res->name);
    }

    res->name = name->clone();
    res->children.push_back(res->name);

    res->set(res->select, select->clone());
    return res;
}

void ASTCreateAlertQuery::formatImpl(
    const IAST::FormatSettings & settings, IAST::FormatState & /*state*/, IAST::FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "ALERT ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "");
    if (database)
        settings.ostr << backQuoteIfNeed(getDatabase()) << ".";

    settings.ostr << backQuoteIfNeed(getName()) << (settings.hilite ? hilite_none : "");

    if (batch_size > 0)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nBATCH " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_keyword : "");
        settings.ostr << batch_size << (batch_size > 1 ? " EVENTS" : " EVENT");
        settings.ostr << " WITH TIMEOUT " << (settings.hilite ? hilite_none : "");
        settings.ostr << batch_timeout << batch_timeout_unit.toAbbreviation();
    }

    if (limit_count > 0)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nLIMIT " << (settings.hilite ? hilite_none : "");
        settings.ostr << limit_count;
        settings.ostr << (settings.hilite ? hilite_keyword : "");
        settings.ostr << (limit_count > 1 ? " ALERTS" : " ALERT");
        settings.ostr << " PER " << (settings.hilite ? hilite_none : "");
        settings.ostr << limit_interval << limit_interval_kind.toAbbreviation();
    }

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nCALL " << (settings.hilite ? hilite_none : "")
                  << backQuoteIfNeed(function_name);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "\nAS " << (settings.hilite ? hilite_none : "");
    select->format(settings);
}

String ASTCreateAlertQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTCreateAlertQuery::getName() const
{
    String result;
    tryGetIdentifierNameInto(name, result);
    return result;
}

String ASTCreateAlertQuery::getSelectQuery() const
{
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings{buf, /*one_line_=*/true};
    select->format(settings);
    return buf.str();
}

}
