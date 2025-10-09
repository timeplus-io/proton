#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP TASK [db.]name
class ParserDropTaskQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP TASK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
