#include <Parsers/Streaming/ASTJavaScriptFunction.h>
#include <Parsers/Streaming/ParserJavaScriptFunction.h>

#include <IO/ReadBufferFromMemory.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/StringUtils/StringUtils.h>

#include <Poco/JSON/Parser.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

bool ParserJavaScriptFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & /*expected*/, [[maybe_unused]] bool hint)
{
    if (!pos.isValid())
        return false;

    /** Query like this:
    * UDA:
    *     $$
    *             has_customized_emit: true,
    *             initialize: function() {...},
    *             process: function(...) {...},
    *             finalize: function() {...},
    *             merge: function(...) {...},
    *             ...
    *     $$
    * UDF:
    *     $$ function add_five(value) {...} $$
    */

    if (pos->type != TokenType::Source)
        throw Exception("Source code of JavaScript function should be quoted by '$$'", ErrorCodes::SYNTAX_ERROR);

    /// skip '$$' in the the begin and end
    String raw_src(pos->begin + 2, pos->size() - 4);
    auto query = std::make_shared<ASTJavaScriptFunction>();
    query->source = raw_src;
    node = query;

    ++pos;

    return true;
}

}
