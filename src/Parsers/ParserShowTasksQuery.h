#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// SHOW TASKS [FROM <db>]
class ParserShowTasksQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "SHOW TASKS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};
}

