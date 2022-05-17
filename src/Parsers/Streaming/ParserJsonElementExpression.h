#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// For examples:
/// 1) raw:a.b              ->      json_value(raw, '$.`a`.`b`')
/// 2) raw:`a.b`.c::int     ->      cast(json_value(raw, '$.`a.b`.`c`), 'int')
class ParserJsonElementExpression : public IParserBase
{
private:
    static const char * operators[];

protected:
    const char * getName() const override { return "json element expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

class ParserJsonElementName : public IParserBase
{
public:
    explicit ParserJsonElementName(bool & is_json_elem_) : is_json_elem(is_json_elem_) {}

private:
    bool & is_json_elem;

protected:
    const char * getName() const override { return "json element name"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
