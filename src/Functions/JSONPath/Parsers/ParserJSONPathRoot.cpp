#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathRoot.h>

#include <Parsers/Lexer.h>

namespace DB
{
/**
 *
 * @param pos token iterator
 * @param node node of ASTJSONPathRoot
 * @param expected stuff for logging
 * @return was parse successful
 */
bool ParserJSONPathRoot::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type != TokenType::DollarSign)
    {
        if (hint)
            expected.add(pos, "dollar sign (start of jsonpath)");
        return false;
    }
    node = std::make_shared<ASTJSONPathRoot>();
    ++pos;
    return true;
}

}
