#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP STORAGE POLICY hcs
class ParserDropStoragePolicyQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP Disk query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
