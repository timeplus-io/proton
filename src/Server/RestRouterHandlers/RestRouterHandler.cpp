#include <Server/RestRouterHandlers/RestRouterHandler.h>

#include <Interpreters/executeSelectQuery.h>

namespace DB
{
namespace
{
Poco::JSON::Object::Ptr parseRequest(HTTPServerRequest & request)
{
    String data = "{}";

    auto size = request.getContentLength();
    if (size > 0)
    {
        data.resize(size);
        request.getStream().readStrict(data.data(), size);
    }

    Poco::JSON::Parser parser;
    return parser.parse(data).extract<Poco::JSON::Object::Ptr>();
}

String buildResponse(const String & query_id, Poco::JSON::Object & resp)
{
    resp.set("request_id", query_id);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

}

/// Execute request and return response. If it failed, a correct
/// `http_status` code will be set by trying best.
/// This function may throw, and caller will need catch the exception
/// and sends back HTTP `500` to clients
void RestRouterHandler::execute(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setupHTTPContext(request);

    /// Database can be from 1) URI path parameter, 2) HTTP header 3) current default database
    /// in highest precedence to lowest precedence, URI path parameters is setup in RestRouterFactory
    /// before this execute is invoked
    {
        auto iter = path_parameters.find("database");
        if (iter == path_parameters.end())
        {
            /// Check if HTTP header has it
            String header_database = request.get("x-timeplus-database", request.get("x-proton-database", ""));
            setCurrentDatabase(std::move(header_database));
        }
        else
        {
            setCurrentDatabase(String{iter->second});
        }
    }

    String header_format = request.get("x-timeplus-format", request.get("x-proton-format", ""));
    if (!header_format.empty())
        query_context->setDefaultFormat(header_format);

    std::pair<String, Int32> result;

    if (streamingInput())
    {
        result = execute(request.getStream());
    }
    else
    {
        auto payload{parseRequest(request)};
        if (streamingOutput())
        {
            /// For keep-alive to work.
            if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
                response.setChunkedTransferEncoding(true);
            execute(payload, response);
        }
        else
        {
            result = execute(payload);
        }
    }

    /// When `streamingOutput` is true, all of the response has already handled
    /// We shall not send any other data any more, otherwise the response stream
    /// can be corrupted
    if (!streamingOutput())
    {
        response.setStatusAndReason(HTTPResponse::HTTPStatus(result.second));
        *response.send() << result.first;
    }
}

void RestRouterHandler::setupDistributedQueryParameters(
    const std::map<String, String> & parameters, const Poco::JSON::Object::Ptr & payload) const
{
    if (payload)
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
    }
    else
    {
        /// keep payload consistent with the schema in interpreters
        query_context->setQueryParameter("_payload", "{}");
    }

    for (const auto & kv : parameters)
        query_context->setQueryParameter(kv.first, kv.second);
}

String RestRouterHandler::processQuery(const String & query, const std::function<void(Block &&)> & callback) const
{
    Poco::JSON::Object resp;
    return processQuery(query, resp, callback);
}

String
RestRouterHandler::processQuery(const String & query, Poco::JSON::Object & resp, const std::function<void(Block &&)> & callback) const
{
    executeNonInsertQuery(query, query_context, callback, false);
    return buildResponse(query_context->getCurrentQueryId(), resp);
}

}
