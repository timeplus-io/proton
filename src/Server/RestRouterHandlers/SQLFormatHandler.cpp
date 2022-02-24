#include "SQLFormatHandler.h"

#include "SchemaValidator.h"

#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/parseQueryPipe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{
    const std::map<String, std::map<String, String>> HIGHLIGHT_SCHEMA = {{"required", {{"query", "string"}}}};
}

bool SQLFormatHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(HIGHLIGHT_SCHEMA, payload, error_msg);
}

std::pair<String, Int32> SQLFormatHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & query = payload->get("query").toString();
    ParserQuery parser(query.c_str() + query.size());
    LOG_INFO(log, "Parse and highlight query: {}", query);

    auto & settings = query_context->getSettingsRef();

    String error_msg;
    auto res = rewriteQueryPipeAndParse(
        parser, query.c_str(), query.c_str() + query.size(), error_msg, false, settings.max_query_size, settings.max_parser_depth);

    if (!error_msg.empty())
    {
        LOG_WARNING(log, "Query rewrite, query_id={} error_msg={}", query_context->getCurrentQueryId(), error_msg);

        return {jsonErrorResponse(error_msg, ErrorCodes::INCORRECT_QUERY), HTTPResponse::HTTP_BAD_REQUEST};
    }

    auto & [rewritten_query, ast] = res;
    LOG_DEBUG(log, "Query rewrite, query_id={} rewritten={}", query_context->getCurrentQueryId(), rewritten_query);

    Poco::JSON::Object resp;
    resp.set("request_id", query_context->getCurrentQueryId());
    resp.set("query", serializeAST(*ast, false));
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

}
