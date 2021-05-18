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
}
