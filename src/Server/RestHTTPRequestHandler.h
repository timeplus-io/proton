#pragma once

#include "IServer.h"

#include <Server/HTTP/HTTPRequestHandler.h>
#include <common/types.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class RestHTTPRequestHandler final : public HTTPRequestHandler
{
public:
    RestHTTPRequestHandler(IServer & server_, const String & name);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    Poco::Logger * log;

    void trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);
};

}
