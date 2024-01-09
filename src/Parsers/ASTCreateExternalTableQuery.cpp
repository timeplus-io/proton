#include <Common/quoteString.h>
#include <Parsers/ASTCreateExternalTableQuery.h>

namespace DB
{

ASTPtr ASTCreateExternalTableQuery::clone() const
{
    auto res = std::make_shared<ASTCreateExternalTableQuery>(*this);

    if (settings)
        res->set(res->settings, settings->clone());

    cloneOutputOptions(*res);
    cloneTableOptions(*res);

    return res;
}

    void ASTCreateExternalTableQuery::formatQueryImpl(const FormatSettings & fmt_settings, FormatState & state, FormatStateStacked frame) const
{
    auto & ostr = fmt_settings.ostr;
    auto hilite = fmt_settings.hilite;

    ostr << (hilite ? hilite_keyword : "")
        << "CREATE "
        << (create_or_replace ? "OR REPLACE " : "")
        << "EXTERNAL STREAM "
        << (if_not_exists ? "IF NOT EXISTS " : "")
        << (hilite ? hilite_none : "")
        << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    if (settings)
    {
        ostr << (hilite ? hilite_keyword : "") << fmt_settings.nl_or_ws << "SETTINGS " << (hilite ? hilite_none : "");
        settings->formatImpl(fmt_settings, state, frame);
    }
}

}
