#include "RestHTTPRequestHandler.h"

#include "RestRouterHandlers/RestRouterFactory.h"
#include "RestRouterHandlers/RestRouterHandler.h"

#include <Access/Authentication.h>
#include <Access/ExternalAuthenticators.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <Common/setThreadName.h>

#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/StreamCopier.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_OPEN_FILE;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;
    extern const int AUTHENTICATION_FAILED;

    extern const int HTTP_LENGTH_REQUIRED;
}

namespace
{
    String base64Decode(const String & encoded)
    {
        String decoded;
        Poco::MemoryInputStream istr(encoded.data(), encoded.size());
        Poco::Base64Decoder decoder(istr);
        Poco::StreamCopier::copyToString(decoder, decoded);
        return decoded;
    }

    String base64Encode(const String & decoded)
    {
        std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ostr.exceptions(std::ios::failbit);
        Poco::Base64Encoder encoder(ostr);
        encoder.rdbuf()->setLineLength(0);
        encoder << decoded;
        encoder.close();
        return ostr.str();
    }

    Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
    {
        using namespace Poco::Net;

        if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        {
            return HTTPResponse::HTTP_UNAUTHORIZED;
        }
        else if (
            exception_code == ErrorCodes::CANNOT_PARSE_TEXT || exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
            || exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING || exception_code == ErrorCodes::CANNOT_PARSE_DATE
            || exception_code == ErrorCodes::CANNOT_PARSE_DATETIME || exception_code == ErrorCodes::CANNOT_PARSE_NUMBER
            || exception_code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST
            || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE || exception_code == ErrorCodes::TOO_DEEP_AST
            || exception_code == ErrorCodes::TOO_BIG_AST || exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE
            || exception_code == ErrorCodes::SYNTAX_ERROR || exception_code == ErrorCodes::INCORRECT_DATA
            || exception_code == ErrorCodes::TYPE_MISMATCH)
        {
            return HTTPResponse::HTTP_BAD_REQUEST;
        }
        else if (
            exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_FUNCTION
            || exception_code == ErrorCodes::UNKNOWN_IDENTIFIER || exception_code == ErrorCodes::UNKNOWN_TYPE
            || exception_code == ErrorCodes::UNKNOWN_STORAGE || exception_code == ErrorCodes::UNKNOWN_DATABASE
            || exception_code == ErrorCodes::UNKNOWN_SETTING || exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING
            || exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION || exception_code == ErrorCodes::UNKNOWN_FORMAT
            || exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
        {
            return HTTPResponse::HTTP_NOT_FOUND;
        }
        else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
        {
            return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
        }
        else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
        {
            return HTTPResponse::HTTP_NOT_IMPLEMENTED;
        }
        else if (exception_code == ErrorCodes::SOCKET_TIMEOUT || exception_code == ErrorCodes::CANNOT_OPEN_FILE)
        {
            return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        }
        else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
        {
            return HTTPResponse::HTTP_LENGTH_REQUIRED;
        }

        return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    }
}

bool RestHTTPRequestHandler::authenticateUser(
    ContextPtr context, HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response)
{
    using namespace Poco::Net;

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("x-daisy-user", "");
    std::string password = request.get("x-daisy-key", "");
    std::string quota_key = request.get("x-daisy-quota", "");

    std::string spnego_challenge;

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            /// It is prohibited to mix different authorization schemes.
            if (params.has("user") || params.has("password"))
                throw Exception(
                    "Invalid authentication: it is not allowed to use Authorization HTTP header and authentication via parameters "
                    "simultaneously",
                    ErrorCodes::AUTHENTICATION_FAILED);

            std::string scheme;
            std::string auth_info;
            request.getCredentials(scheme, auth_info);

            if (Poco::icompare(scheme, "Basic") == 0)
            {
                HTTPBasicCredentials credentials(auth_info);
                user = credentials.getUsername();
                password = credentials.getPassword();
            }
            else if (Poco::icompare(scheme, "Negotiate") == 0)
            {
                spnego_challenge = auth_info;

                if (spnego_challenge.empty())
                    throw Exception("Invalid authentication: SPNEGO challenge is empty", ErrorCodes::AUTHENTICATION_FAILED);
            }
            else
            {
                throw Exception(
                    "Invalid authentication: '" + scheme + "' HTTP Authorization scheme is not supported",
                    ErrorCodes::AUTHENTICATION_FAILED);
            }
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials() || params.has("user") || params.has("password") || params.has("quota_key"))
            throw Exception(
                "Invalid authentication: it is not allowed to use x-daisy HTTP headers and other authentication methods "
                "simultaneously",
                ErrorCodes::AUTHENTICATION_FAILED);
    }

    if (spnego_challenge.empty()) // I.e., now using user name and password strings ("Basic").
    {
        if (!request_credentials)
            request_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(request_credentials.get());
        if (!basic_credentials)
            throw Exception("Invalid authentication: unexpected 'Basic' HTTP Authorization scheme", ErrorCodes::AUTHENTICATION_FAILED);

        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }
    else
    {
        if (!request_credentials)
            request_credentials = request_context->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(request_credentials.get());
        if (!gss_acceptor_context)
            throw Exception(
                "Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected", ErrorCodes::AUTHENTICATION_FAILED);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma GCC diagnostic pop

        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception("Invalid authentication: 'Negotiate' HTTP Authorization failure", ErrorCodes::AUTHENTICATION_FAILED);

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            return false;
        }
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

    ClientInfo & client_info = context->getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::HTTP;

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;
    else if (request.getMethod() == HTTPServerRequest::HTTP_PATCH)
        http_method = ClientInfo::HTTPMethod::PATCH;
    else if (request.getMethod() == HTTPServerRequest::HTTP_DELETE)
        http_method = ClientInfo::HTTPMethod::DELETE;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");

    try
    {
        context->setUser(*request_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        request_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        request_credentials = request_context->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }

    request_credentials.reset();

    if (!quota_key.empty())
        context->setQuotaKey(quota_key);

    /// Query sent through HTTP interface is initial.
    client_info.initial_user = client_info.current_user;
    client_info.initial_address = client_info.current_address;

    return true;
}

void RestHTTPRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("RestHandler");
    ThreadStatus thread_status;

    SCOPE_EXIT({
        // If there is no request_credentials instance waiting for the next round, then the request is processed,
        // so no need to preserve request_context either.
        // Needs to be performed with respect to the other destructors in the scope though.
        if (!request_credentials)
            request_context.reset();
    });

    if (!request_context)
    {
        // Context should be initialized before anything, for correct memory accounting.
        request_context = Context::createCopy(server.context());
        request_credentials.reset();
    }

    HTMLForm params(request);
    LOG_TRACE(log, "Request uri: {}", request.getURI());

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    ClientInfo & client_info = request_context->getClientInfo();
    request_context->setCurrentQueryId(request.get("x-daisy-request-id", request.get("x-daisy-query-id", "")));
    client_info.initial_query_id = client_info.current_query_id;

    /// Setup idemopotent key if it is passed by user
    String idem_key = request.get("x-daisy-idempotent-id", "");

    if (!idem_key.empty())
    {
        request_context->setIdempotentKey(idem_key);
    }

    CurrentThread::QueryScope query_scope{request_context};
    /// Setup common response headers etc
    response.setContentType("application/json; charset=UTF-8");
    response.add("x-daisy-query-id", request_context->getCurrentQueryId());

    /// Set keep alive timeout
    const auto & config = server.config();

    setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));

    try
    {
        if (!authenticateUser(request_context, request, params, response))
            return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

        const auto & database = getDatabaseByUser(client_info.current_user);
        request_context->setCurrentDatabase(database);

        auto router_handler = RestRouterFactory::instance().get(request.getURI(), request.getMethod(), request_context);
        if (router_handler == nullptr)
        {
            response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
            const auto & resp
                = RestRouterHandler::jsonErrorResponse("Unknown URI", ErrorCodes::UNKNOWN_TYPE_OF_QUERY, client_info.current_query_id);
            *response.send() << resp << std::endl;
            return;
        }

        LOG_DEBUG(log, "Start processing query_id={} user={}", client_info.current_query_id, client_info.current_user);
        router_handler->execute(request, response);
        LOG_DEBUG(log, "End of processing query_id={} user={}", client_info.current_query_id, client_info.current_user);
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        int exception_code = getCurrentExceptionCode();

        const auto & resp
            = RestRouterHandler::jsonErrorResponse(getCurrentExceptionMessage(false, true), exception_code, client_info.current_query_id);

        trySendExceptionToClient(resp, exception_code, request, response);
    }
}

RestHTTPRequestHandler::RestHTTPRequestHandler(IServer & server_, const String & name) : server(server_), log(&Poco::Logger::get(name))
{
}

/// FIXME : Get the corresponding database according to the user name
String RestHTTPRequestHandler::getDatabaseByUser(const String & user) const
{
    String database = "default";

    /// FIXME : HACK, fix this when we have a user to database mapping.
    if (user == "system")
    {
        return "system";
    }

    if (database.empty())
    {
        throw Exception(
            "Unknown database: Cannot find database information corresponding to user '" + user + "' ", ErrorCodes::UNKNOWN_DATABASE);
    }

    return database;
}

void RestHTTPRequestHandler::trySendExceptionToClient(
    const String & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        response.set("x-daisy-exception-code", std::to_string(exception_code));

        /// FIXME: make sure that no one else is reading from the same stream at the moment.

        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && response.getKeepAlive()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED && !request.getStream().eof())
        {
            request.getStream().ignoreAll();
        }

        bool auth_fail = exception_code == ErrorCodes::UNKNOWN_USER || exception_code == ErrorCodes::WRONG_PASSWORD
            || exception_code == ErrorCodes::REQUIRED_PASSWORD;

        if (auth_fail)
        {
            response.requireAuthentication("Daisy server HTTP API");
        }
        else
        {
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
        }

        if (!response.sent())
        {
            /// If nothing was sent yet and we don't even know if we must compress the response.
            *response.send() << s << std::endl;
        }
        else
        {
            assert(false);
            __builtin_unreachable();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}

}
