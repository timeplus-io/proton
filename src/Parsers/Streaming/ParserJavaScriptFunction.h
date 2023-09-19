#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/** Query like this:
  * UDA:
  *     $$
  *             has_customized_emit: true,
  *             initialize: function() {...},
  *             process: function(...) {...},
  *             finalize: function() {...},
  *             merge: function(...) {...},
  *             ...
  *     $$
  * UDF:
  *     $$ function add_five(value) {...} $$
  */
class ParserJavaScriptFunction : public IParserBase
{
private:
    const char * getName() const override { return "Javascript Function query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};

}
