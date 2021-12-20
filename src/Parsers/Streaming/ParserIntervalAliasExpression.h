#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// Optional conversion to interval alias. Example:
/// 1) "xs" parsed as "toIntervalSecond(x)"
/// 2) "ym" parsed as "toIntervalMinute(y)"
/// 3) "zh" parsed as "toIntervalHour(z)"
/// now kind supports: 's','m','h'
class ParserIntervalAliasExpression : public IParserBase
{
protected:
    const char * getName() const  override { return "interval alias expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
