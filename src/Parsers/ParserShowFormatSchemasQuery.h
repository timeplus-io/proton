#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/// Parses queries like
///   SHOW FORMAT SCHEMA name [TYPE schema_type]
class ParserShowFormatSchemasQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW FORMAT SCHEMAS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
