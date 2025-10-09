#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query like this:
  * SHOW STORAGE POLICIES [[NOT] [I]LIKE 'str'] [LIMIT expr]
  * or
  * SHOW STORAGE POLICIES WHERE <predict>.
  */
class ParserShowStoragePoliciesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW STORAGE POLICIES [[NOT] [I]LIKE 'str'] [LIMIT expr]"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
