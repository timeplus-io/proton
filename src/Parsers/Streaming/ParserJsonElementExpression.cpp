#include "ParserJsonElementExpression.h"

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{
const char * ParserJsonElementExpression::operators[] = {":", "json_value", nullptr};

bool ParserJsonElementExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    auto pos_begin = pos;
    bool is_json_elem = false;
    bool parsed
        = ParserLeftAssociativeBinaryOperatorList{operators, std::make_unique<ParserExpressionElement>(), std::make_unique<ParserJsonElementName>(is_json_elem)}
              .parse(pos, node, expected);

    /// Set code name: 'raw:obj.data' 'raw:test' ...
    if (is_json_elem)
    {
        auto * func = node->as<ASTFunction>();
        assert(func);
        for (auto iter = pos_begin; iter != pos; ++iter)
            func->code_name.append(iter->begin, iter->size());
    }

    return parsed;
}

bool ParserJsonElementName::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ASTPtr elem_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Dot), false)
             .parse(pos, elem_list, expected))
    {
        is_json_elem = false;
        return false;
    }

    /// For examples: { "a.b": { "c": 1 } }
    /// Convert `a.b`.c -> $.`a.b`.`c`
    const auto & list = elem_list->as<ASTExpressionList &>();
    std::vector<String> elems;
    elems.reserve(list.children.size() + 1);
    elems.emplace_back("$");
    for (const auto & child : list.children)
        elems.emplace_back(backQuote(getIdentifierName(child)));

    node = std::make_shared<ASTLiteral>(boost::algorithm::join(elems, "."));

    is_json_elem = true;
    return true;
}
}
