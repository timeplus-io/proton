#include "ParserIntervalAliasExpression.h"

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Common/IntervalKind.h>

namespace DB
{
bool ParserIntervalAliasExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!pos.isValid())
        return false;

    /// Case: 1s
    /// Parse number
    Int64 x = 0;
    ReadBufferFromMemory in(pos->begin, pos->size());
    if (!tryReadIntText(x, in) || in.count() == pos->size())
        return false;

    /// Parse interval kind
    IntervalKind interval_kind;
    {
        Tokens kind_token(pos->begin + in.count(), pos->begin + pos->size());
        Pos kind_pos(kind_token, 0);
        if (ParserKeyword("S").ignore(kind_pos, expected))
            interval_kind = IntervalKind::Second;
        else if (ParserKeyword("M").ignore(kind_pos, expected))
            interval_kind = IntervalKind::Minute;
        else if (ParserKeyword("H").ignore(kind_pos, expected))
            interval_kind = IntervalKind::Hour;
        else
            return false;
    }

    auto expr = x < 0 ? std::make_shared<ASTLiteral>(Int64(x)) : std::make_shared<ASTLiteral>(UInt64(x));
    expr->begin = pos;
    expr->end = ++pos;

    /// The function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// Function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// The first argument of the function is the previous element, the second is the next one
    function->name = interval_kind.toNameOfFunctionToIntervalDataType();
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    exp_list->children.push_back(expr);

    node = function;
    return true;
}
}
