#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query SHOW CREATE ALERT [db.]name [SETTINGS settings]
  */
class ParserShowCreateAlertQuery: public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE ALERT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
