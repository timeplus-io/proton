#include <Common/quoteString.h>
#include "Parsers/ASTIdentifier_fwd.h"
#include <IO/Operators.h>
#include <Parsers/ASTCreateFormatSchemaQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
ASTPtr ASTCreateFormatSchemaQuery::clone() const
{
    auto res = std::make_shared<ASTCreateFormatSchemaQuery>(*this);
    res->children.clear();

    res->schema_name = schema_name->clone();
    res->children.push_back(res->schema_name);

    res->schema_core = schema_core->clone();
    res->children.push_back(res->schema_core);
    return res;
}

void ASTCreateFormatSchemaQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &  /*state*/, IAST::FormatStateStacked  /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "FORMAT SCHEMA ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getSchemaName()) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");

    /// Do not format the source of the schema.
    settings.ostr << fmt::format("\n$$\n{}\n$$\n", this->getSchemaBody());

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "TYPE ";
    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(schema_type) << (settings.hilite ? hilite_none : "");
}

String ASTCreateFormatSchemaQuery::getSchemaName() const
{
    String name;
    tryGetIdentifierNameInto(schema_name, name);
    return name;
}

String ASTCreateFormatSchemaQuery::getSchemaBody() const
{
    ASTLiteral * schema_body = schema_core->as<ASTLiteral>();
    return schema_body->value.safeGet<String>();
}
}
