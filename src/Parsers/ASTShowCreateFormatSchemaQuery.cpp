#include <Common/quoteString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowCreateFormatSchemaQuery.h>

namespace DB
{
String ASTShowCreateFormatSchemaQuery::getSchemaName() const
{
    String name;
    tryGetIdentifierNameInto(schema_name, name);
    return name;
}

void ASTShowCreateFormatSchemaQuery::formatQueryImpl(const FormatSettings & settings, FormatState &  /*state*/, FormatStateStacked  /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE FORMAT SCHEMA" << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << backQuoteIfNeed(getSchemaName());
    if (!schema_type.empty())
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TYPE " << (settings.hilite ? hilite_none : "") << schema_type;
}
}
