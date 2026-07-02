#include <Server/WebUIRequestHandler.h>
#include "IServer.h"

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>

#include <string_view>

/// The play UI page is shipped alongside the server binary via C23 `#embed`.
constexpr unsigned char resource_play_html[] = {
#embed "../../programs/server/play.html"
};


namespace DB
{

WebUIRequestHandler::WebUIRequestHandler(IServer & server_, std::string resource_name_)
    : server(server_), resource_name(std::move(resource_name_))
{
}

void WebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);

    std::string_view html;
    if (resource_name == "play.html")
        html = std::string_view(reinterpret_cast<const char *>(resource_play_html), std::size(resource_play_html));

    WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
}
}
