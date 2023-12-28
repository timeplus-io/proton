#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTShowFormatSchemasQuery.h>


namespace DB
{
void ASTShowFormatSchemasQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW FORMAT SCHEMAS"
                  << (settings.hilite ? hilite_none : "");

    if (!schema_type.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TYPE "
                      << (settings.hilite ? hilite_none : "");
        settings.ostr << schema_type;
    }
}
}
