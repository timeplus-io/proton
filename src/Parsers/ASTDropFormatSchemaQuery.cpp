#include <Parsers/ASTDropFormatSchemaQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFormatSchemaQuery::clone() const
{
    return std::make_shared<ASTDropFormatSchemaQuery>(*this);
}

void ASTDropFormatSchemaQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP FORMAT SCHEMA ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(schema_name) << (settings.hilite ? hilite_none : "");
    if (!schema_type.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TYPE " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << schema_type << (settings.hilite ? hilite_none : "");
    }
    formatOnCluster(settings);
}

}
