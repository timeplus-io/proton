#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
/** Query like this:
  * EMIT [STREAM] PERIODIC INTERVAL '3' SECONDS
  * EMIT [STREAM] AFTER DELAY INTERVAL '3' SECONDS
  * EMIT [STREAM] AFTER WATERMARK
  * EMIT [STREAM] AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
  */
class ParserEmitQuery : public IParserBase
{
public:
    explicit ParserEmitQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) { }

private:
    const char * getName() const override { return "Emit query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool parse_only_internals;
};

}
