#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// SHOW ALERTS [FROM <db>]
class ParserShowAlertsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW ALERTS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};
}
