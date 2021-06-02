#include "RestRouterHandler.h"

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
    if (!isDistributedDDL())
    {
        return;
    }

    if (payload)
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
    }
    else
    {
        /// keep payload consistent with the schema in interper interpreters
        query_context->setQueryParameter("_payload", "{}");
    }

    for (const auto & kv : parameters)
    {
        query_context->setQueryParameter(kv.first, kv.second);
    }
    query_context->setDistributedDDLOperation(true);
}

String RestRouterHandler::processQuery(const String & query, const std::function<void(Block &&)> & callback) const
{
    Poco::JSON::Object resp;
    return processQuery(query, resp, callback);
}

String
RestRouterHandler::processQuery(const String & query, Poco::JSON::Object & resp, const std::function<void(Block &&)> & callback) const
{
    executeSelectQuery(query, query_context, callback, false);
    return buildResponse(query_context->getCurrentQueryId(), resp);
}

}
