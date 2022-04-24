#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathStar : public IParserBase
{
private:
    const char * getName() const override { return "ParserJSONPathStar"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;

public:
    explicit ParserJSONPathStar() = default;
};

}
