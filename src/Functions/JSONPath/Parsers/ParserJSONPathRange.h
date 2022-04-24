#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathRange : public IParserBase
{
private:
    const char * getName() const override { return "ParserJSONPathRange"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;

public:
    explicit ParserJSONPathRange() = default;
};

}
