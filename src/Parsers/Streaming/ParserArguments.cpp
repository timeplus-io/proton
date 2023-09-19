#include <Parsers/Streaming/ParserArguments.h>

#include <Parsers/Streaming/ParserFuncArgument.h>

#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

bool ParserArguments::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!pos.isValid())
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;

    ++pos;

    ParserList arg_list_parser(std::make_unique<ParserFuncArgument>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr expr_list_args;
    if (!arg_list_parser.parse(pos, expr_list_args, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;

    ++pos;

    auto function = std::make_shared<ASTFunctionWithKeyValueArguments>(true);
    function->ignore_name = true;
    function->elements = expr_list_args;
    function->children.push_back(function->elements);

    node = function;

    return true;
}

}
