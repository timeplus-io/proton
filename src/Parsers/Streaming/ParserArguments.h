#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/** Query like this used as UDF argument list:
  * (value float32, time datetime64(3))
  */
class ParserArguments : public IParserBase
{
private:
    const char * getName() const override { return "Arguments query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
