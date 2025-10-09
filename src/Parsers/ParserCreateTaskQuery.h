#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/** Parses queries like
  * CREATE [OR REPLACE] TASK [IF NOT EXISTS] [db.]name
  *     SCHEDULE '0/5 * * * * UTC'
  *     [ TIMEOUT (1s | 1m | 1h) ] -- default: 1 hour
  * AS
  *     <sql>
  */
class ParserCreateTaskQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "CREATE TASK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}

