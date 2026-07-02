#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{
class ParserShowCreateDiskQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE DISK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint) override;
};
}
