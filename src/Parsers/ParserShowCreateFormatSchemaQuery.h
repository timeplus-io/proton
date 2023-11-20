#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query SHOW CREATE FORMAT SCHEMA name [TYPE type] [FORMAT format]
  */
class ParserShowCreateFormatSchemaQuery: public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE FORMAT SCHEMA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
