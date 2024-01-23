#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
/** Query like this:
  * EMIT [STREAM|CHANGELOG] PERIODIC INTERVAL '3' SECONDS
  * EMIT [STREAM|CHANGELOG] AFTER WATERMARK WITH DELAY INTERVAL '3' SECONDS
  * EMIT [STREAM|CHANGELOG] AFTER WATERMARK
  * EMIT [STREAM|CHANGELOG] AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
  * EMIT [STREAM|CHANGELOG] LAST 1h ON PROCTIME
  */
class ParserEmitQuery : public IParserBase
{
public:
    explicit ParserEmitQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) { }

private:
    const char * getName() const override { return "Emit query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;

private:
    bool parse_only_internals;
};

}
