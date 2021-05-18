#include "RestRouterHandler.h"

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
}

/// Execute request and return response. If it failed, a correct
/// `http_status` code will be set by trying best.
/// This function may throw, and caller will need catch the exception
/// and sends back HTTP `500` to clients
void RestRouterHandler::execute(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setupHTTPContext(request);

    Int32 http_status = HTTPResponse::HTTP_OK;
    String response_payload;

    if (streamingInput())
    {
        response_payload = execute(request.getStream(), http_status);
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
            response_payload = execute(payload, http_status);
    }

    /// When `streamingOutput` is true, all of the response has already handled
    /// We shall not send any other data any more, otherwise the response stream
    /// can be corrupted
    if (!streamingOutput())
    {
        response.setStatusAndReason(HTTPResponse::HTTPStatus(http_status));
        *response.send() << response_payload << std::endl;
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

}
