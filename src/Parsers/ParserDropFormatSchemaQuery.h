#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP FORMAT SCHEMA foo
class ParserDropFormatSchemaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP FORMAT SCHEMA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
