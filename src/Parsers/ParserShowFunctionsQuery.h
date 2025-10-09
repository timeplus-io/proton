#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// Parses queryies like
///   SHOW FUNCTIONS [Where filter_expression]
class ParserShowFunctionsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW FUNCTIONS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
};
}
