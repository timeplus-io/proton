#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query like this:
  * SHOW DISKS [[NOT] [I]LIKE 'str'] [LIMIT expr]
  * or
  * SHOW DISK WHERE <predict>.
  */
class ParserShowDisksQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW DISKS [[NOT] [I]LIKE 'str'] [LIMIT expr]"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
