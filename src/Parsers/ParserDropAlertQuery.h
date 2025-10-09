#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP ALERT [db.]name
class ParserDropAlertQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP ALERT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
