#include <Server/NotFoundHandler.h>

#include <Common/Exception.h>

namespace DB
{
void NotFoundHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "There is no HTTP handle for path " << request.getURI() << "\n\n";
    }
    catch (...)
    {
        tryLogCurrentException("NotFoundHandler");
    }
}

}
