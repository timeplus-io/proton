#include <Parsers/ASTShowTasksQuery.h>

#include <IO/Operators.h>
#include <Common/quoteString.h>


namespace DB
{
ASTPtr ASTShowTasksQuery::clone() const
{
    return std::make_shared<ASTShowTasksQuery>(*this);
}

void ASTShowTasksQuery::formatQueryImpl(const FormatSettings & fmt_settings, FormatState &, FormatStateStacked) const
{
    fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << "SHOW TASKS" << (fmt_settings.hilite ? hilite_none : "");

    if (database)
    {
        fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << " FROM " << (fmt_settings.hilite ? hilite_none : "");
        fmt_settings.ostr << *database;
    }
}
}

