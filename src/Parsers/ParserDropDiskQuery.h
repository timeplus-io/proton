#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP DISK s3_disk
class ParserDropDiskQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP Disk query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
