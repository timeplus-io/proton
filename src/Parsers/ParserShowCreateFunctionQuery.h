#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{

/// Query SHOW CREATE FUNCTION <function_name>
class ParserShowCreateFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
