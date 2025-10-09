#include <IO/Operators.h>
#include <Parsers/ASTShowAlertsQuery.h>
#include <Common/quoteString.h>


namespace DB
{
ASTPtr ASTShowAlertsQuery::clone() const
{
    return std::make_shared<ASTShowAlertsQuery>(*this);
}

void ASTShowAlertsQuery::formatQueryImpl(const FormatSettings & fmt_settings, FormatState &, FormatStateStacked) const
{
    fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << "SHOW ALERTS" << (fmt_settings.hilite ? hilite_none : "");

    if (database)
    {
        fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << " FROM " << (fmt_settings.hilite ? hilite_none : "");
        fmt_settings.ostr << *database;
    }
}
}

