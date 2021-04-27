#pragma once

#include "IServer.h"

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <common/types.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class Credentials;
class RestHTTPRequestHandler final : public HTTPRequestHandler
{
public:
    RestHTTPRequestHandler(IServer & server_, const String & name);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    Poco::Logger * log;

    // The request_context and the request_credentials instances may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    ContextPtr request_context;
    std::unique_ptr<Credentials> request_credentials;

    // Returns true when the user successfully authenticated,
    //  the request_context instance will be configured accordingly, and the request_credentials instance will be dropped.
    // Returns false when the user is not authenticated yet, and the 'Negotiate' response is sent,
    //  the request_context and request_credentials instances are preserved.
    // Throws an exception if authentication failed.
    bool authenticateUser(
        ContextPtr context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        HTTPServerRequest & request,
        HTTPServerResponse & response);
};

}
