
#include "ParserSessionRangeComparisonExpressionIfPossible.h"

#include "ASTSessionRangeComparision.h"

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
/// Parse range between predications that include/exclude boundary
/// [<start_cond>, <end_cond>]
/// [<start_cond>, <end_cond>)
/// (<start_cond>, <end_cond>]
/// (<start_cond>, <end_cond>)
bool ParserSessionRangeComparisonExpressionIfPossible::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    Pos begin = pos;
    do
    {
        auto range_comp = std::make_shared<ASTSessionRangeComparision>();
        /// Opening
        if (pos->type == TokenType::OpeningSquareBracket)
            range_comp->start_with_inclusion = true;
        else if (pos->type == TokenType::OpeningRoundBracket)
            range_comp->start_with_inclusion = false;
        else
            break;

        ++pos;

        /// Parsing <start predication, end predication>
        ASTPtr conds;
        if (!ParserList(std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma))
                    .parse(pos, conds, expected, hint))
            break;

        auto & cond_list = conds->as<ASTExpressionList &>();
        if (cond_list.children.size() != 2)
            break;

        range_comp->children = std::move(cond_list.children);

        /// Closing
        if (pos->type == TokenType::ClosingSquareBracket)
            range_comp->end_with_inclusion = true;
        else if (pos->type == TokenType::ClosingRoundBracket)
            range_comp->end_with_inclusion = false;
        else
            break;

        ++pos;
        node = std::move(range_comp);
        return true;
    } while (false);

    pos = begin;
    return elem_parser->parse(pos, node, expected, hint);
};
}
