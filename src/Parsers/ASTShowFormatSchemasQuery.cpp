#include <IO/Operators.h>
#include <Parsers/ASTShowFormatSchemasQuery.h>
#include <Common/quoteString.h>


namespace DB
{
ASTPtr ASTShowFormatSchemasQuery::clone() const
{
    return std::make_shared<ASTShowFormatSchemasQuery>(*this);
}

void ASTShowFormatSchemasQuery::formatQueryImpl(const FormatSettings & fmt_settings, FormatState &, FormatStateStacked) const
{
    fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << "SHOW FORMAT SCHEMAS" << (fmt_settings.hilite ? hilite_none : "");

    if (!schema_type.empty())
    {
        fmt_settings.ostr << (fmt_settings.hilite ? hilite_keyword : "") << " TYPE " << (fmt_settings.hilite ? hilite_none : "");
        fmt_settings.ostr << schema_type;
    }
}
}
