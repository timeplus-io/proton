#include <Parsers/Streaming/ParserFuncArgument.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDataType.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

bool ParserFuncArgument::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!pos.isValid())
        return false;

    ParserIdentifier name_parser;
    ParserDataType type_parser;

    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    ASTPtr type;
    if (!type_parser.parse(pos, type, expected))
        return false;

    auto arg = std::make_shared<ASTNameTypePair>();
    tryGetIdentifierNameInto(name, arg->name);
    arg->type = type;

    node = arg;

    return true;
}

}
