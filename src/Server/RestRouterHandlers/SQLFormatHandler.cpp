#include "SQLFormatHandler.h"

#include "SchemaValidator.h"

#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

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

    try
    {
        /// Use standard parseQuery instead of rewriteQueryPipeAndParse for regular SQL formatting
        auto ast = parseQuery(
            parser, query.c_str(), query.c_str() + query.size(), "sqlformat", settings.max_query_size, settings.max_parser_depth);

        if (!ast)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Failed to parse query: empty AST returned");

        LOG_DEBUG(log, "Query parse successful, query_id={} AST_type={}", query_context->getCurrentQueryId(), ast->getID());

        Poco::JSON::Object resp;
        resp.set("request_id", query_context->getCurrentQueryId());
        resp.set("query", serializeAST(*ast, false));
        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);

        return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Query parse failed, query_id={} error_msg={}", query_context->getCurrentQueryId(), e.message());
        return {jsonErrorResponse(e.message(), ErrorCodes::INCORRECT_QUERY), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

}
