#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/** Query like this used as UDF argument list:
  *     complex tuple(float32, datetime64(3))
  */
class ParserFuncArgument : public IParserBase
{
private:
    const char * getName() const override { return "Function Argument query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
