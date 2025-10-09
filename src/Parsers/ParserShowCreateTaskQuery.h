#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query SHOW CREATE TASK [db.]name [SETTINGS settings]
  */
class ParserShowCreateTaskQuery: public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "SHOW CREATE TASK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
