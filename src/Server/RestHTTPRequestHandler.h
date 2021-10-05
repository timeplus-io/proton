#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <Common/CurrentThread.h>
#include <common/types.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class IServer;
class Credentials;
class Session;

class RestHTTPRequestHandler final : public HTTPRequestHandler
{
public:
    RestHTTPRequestHandler(IServer & server_, const String & name);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    /// Reference to the immutable settings in the global context.
    /// Those settings are used only to extract a http request's parameters.
    /// See settings http_max_fields, http_max_field_name_size, http_max_field_value_size in HTMLForm.
    const Settings & default_settings;
    Poco::Logger * log;

    // session is reset at the end of each request/response.
    std::unique_ptr<Session> session;

    // The request_credential instance may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    std::unique_ptr<Credentials> request_credentials;

    // Returns true when the user successfully authenticated,
    //  the session instance will be configured accordingly, and the request_credentials instance will be dropped.
    // Returns false when the user is not authenticated yet, and the 'Negotiate' response is sent,
    //  the session and request_credentials instances are preserved.
    // Throws an exception if authentication failed.
    bool authenticateUser(
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        HTTPServerRequest & request,
        HTTPServerResponse & response);

    String getDatabaseByUser(const String & user) const;

    void processQuery(
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        std::optional<CurrentThread::QueryScope> & query_scope);
};

}
