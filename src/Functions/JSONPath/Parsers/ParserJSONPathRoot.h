#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathRoot : public IParserBase
{
private:
    const char * getName() const override { return "ParserJSONPathRoot"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;

public:
    explicit ParserJSONPathRoot() = default;
};

}
