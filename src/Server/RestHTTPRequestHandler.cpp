#include <Server/RestHTTPRequestHandler.h>
#include <Server/IServer.h>
#include <Access/LocalApiToken.h>

#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Server/RestRouterHandlers/RestRouterFactory.h>
#include <Server/RestRouterHandlers/RestRouterHandler.h>

#include <Access/Authentication.h>
#include <Access/ExternalAuthenticators.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Session.h>
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
extern const int AUTHENTICATION_FAILED;
extern const int HTTP_LENGTH_REQUIRED;
extern const int INVALID_SESSION_TIMEOUT;
extern const int REQUIRED_PASSWORD;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_TYPE_OF_QUERY;
extern const int UNKNOWN_USER;
extern const int WRONG_PASSWORD;
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

std::chrono::steady_clock::duration parseSessionTimeout(const Poco::Util::AbstractConfiguration & config, const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Invalid session timeout: '{}'", session_timeout_str);

        if (session_timeout > max_session_timeout)
            throw Exception(
                ErrorCodes::INVALID_SESSION_TIMEOUT,
                "Session timeout '{}' is larger than max_session_timeout: {}. Maximum session timeout could be modified in configuration "
                "file.",
                session_timeout_str,
                max_session_timeout);
    }

    return std::chrono::seconds(session_timeout);
}
}

RestHTTPRequestHandler::~RestHTTPRequestHandler() = default;

bool RestHTTPRequestHandler::authenticateUser(HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response)
{
    using namespace Poco::Net;

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("x-timeplus-user", request.get("x-proton-user", ""));
    std::string password = request.get("x-timeplus-key", request.get("x-proton-key", ""));
    std::string quota_key = request.get("x-timeplus-quota", request.get("x-proton-quota", ""));

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
                    ErrorCodes::AUTHENTICATION_FAILED,
                    "Invalid authentication: it is not allowed to use Authorization HTTP header and authentication via parameters "
                    "simultaneously");

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
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: SPNEGO challenge is empty");
            }
            else
            {
                throw Exception(
                    ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: '{}' HTTP Authorization scheme is not supported", scheme);
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
                ErrorCodes::AUTHENTICATION_FAILED,
                "Invalid authentication: it is not allowed to use HTTP headers and other authentication methods simultaneously");
    }

    if (spnego_challenge.empty()) // I.e., now using username and password strings ("Basic").
    {
        if (!request_credentials)
            request_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(request_credentials.get());
        if (!basic_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: unexpected 'Basic' HTTP Authorization scheme");

        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }
    else
    {
        if (!request_credentials)
            request_credentials = server.context()->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(request_credentials.get());
        if (!gss_acceptor_context)
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected");

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma GCC diagnostic pop

        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: 'Negotiate' HTTP Authorization failure");

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            return false;
        }
    }

    /// The local API user may only connect from loopback — reject all other origins
    /// before authentication so the token is never tested against a remote attempt.
    if (LocalApiToken::isLocalApiTokenUser(user))
    {
        if (!request.clientAddress().host().isLoopback())
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "User '{}' is restricted to localhost connections",
                user);
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.
    ClientInfo & client_info = session->getClientInfo();
    /// client_info.interface = ClientInfo::Interface::HTTP;
    /// Query sent through HTTP interface is initial.
    /// client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    /// client_info.initial_user = client_info.current_user;
    /// client_info.initial_address = client_info.current_address;

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
    client_info.quota_key = quota_key;

    try
    {
        /// proton: starts. Issue: https://github.com/timeplus-io/proton-enterprise/issues/4740
        /// Bypass some request check
        bool bypass_auth = request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET && isBypassUri(request.getURI());
        if (!bypass_auth)
            session->authenticate(*request_credentials, request.clientAddress());
        /// proton: ends
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
        request_credentials = server.context()->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }

    request_credentials.reset();
    return true;
}

void RestHTTPRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    setThreadName("RestHandler");
    ThreadStatus thread_status;

    LOG_TRACE(log, "Request uri: {}", request.getURI());

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP);
    SCOPE_EXIT({ session.reset(); });
    std::optional<CurrentThread::QueryScope> query_scope;

    response.setContentType("application/json; charset=UTF-8");

    /// For keep-alive to work.
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    try
    {
        HTMLForm params(default_settings, request);
        processQuery(request, params, response, query_scope);
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        int exception_code = getCurrentExceptionCode();

        const auto & resp = RestRouterHandler::jsonErrorResponse(
            getCurrentExceptionMessage(false, true), exception_code, session->getClientInfo().current_query_id);

        trySendExceptionToClient(resp, exception_code, request, response);
    }
}

void RestHTTPRequestHandler::processQuery(
    HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response, std::optional<CurrentThread::QueryScope> & query_scope)
{
    using namespace Poco::Net;

    if (!authenticateUser(request, params, response))
        return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    if (session_is_set)
    {
        session_id = params.get("session_id");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");
        session->makeSessionContext(session_id, session_timeout, session_check == "1");
    }

    // Parse the OpenTelemetry traceparent header.
    // Disable in Arcadia -- it interferes with the
    // test_clickhouse.TestTracing.test_tracing_via_http_proxy[traceparent] test.
    ClientInfo client_info = session->getClientInfo();
#if !defined(ARCADIA_BUILD)
    if (request.has("traceparent"))
    {
        std::string opentelemetry_traceparent = request.get("traceparent");
        std::string error;
        if (!client_info.client_trace_context.parseTraceparentHeader(opentelemetry_traceparent, error))
        {
            throw Exception(
                ErrorCodes::BAD_REQUEST_PARAMETER,
                "Failed to parse OpenTelemetry traceparent header '{}': {}",
                opentelemetry_traceparent,
                error);
        }
        client_info.client_trace_context.tracestate = request.get("tracestate", "");
    }
#endif

    auto context = session->makeQueryContext(std::move(client_info));
    context->setCurrentQueryId(request.get(
        "x-timeplus-request-id",
        request.get("x-timeplus-query-id", request.get("x-proton-request-id", request.get("x-proton-query-id", "")))));
    response.add("x-timeplus-query-id", context->getCurrentQueryId());

    if (is_snapshot_mode)
        context->setSetting("query_mode", Field("table"));

    if (is_clickhouse_compatible_mode)
        context->setSetting("is_clickhouse_compatible", Field(true));

    /// Setup idempotent key if it is passed by user
    String idem_key = request.get("x-timeplus-idempotent-id", request.get("x-proton-idempotent-id", ""));
    if (!idem_key.empty())
    {
        context->setIdempotentKey(idem_key);
    }

    /// Set keep alive timeout
    setResponseDefaultHeaders(response);

    if (!request.getURI().starts_with("/proton/metastore"))
    {
        const auto & database = getDatabaseByUser(context->getUserName());
        context->setCurrentDatabase(database);
    }

    auto router_handler = RestRouterFactory::instance().get(request.getURI(), request.getMethod(), context);
    if (router_handler == nullptr)
    {
        response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
        const auto & resp
            = RestRouterHandler::jsonErrorResponse("Unknown URI", ErrorCodes::UNKNOWN_TYPE_OF_QUERY, context->getCurrentQueryId());
        *response.send() << resp << "\n";
        return;
    }

    LOG_DEBUG(log, "Start processing query_id={} user={}", context->getCurrentQueryId(), context->getUserName());
    query_scope.emplace(context);
    router_handler->execute(request, response);
    LOG_DEBUG(log, "End of processing query_id={} user={}", context->getCurrentQueryId(), context->getUserName());
}

RestHTTPRequestHandler::RestHTTPRequestHandler(
    IServer & server_, const String & name, bool is_snapshot_mode_, bool is_clickhouse_compatible_mode_)
    : server(server_)
    , default_settings(server.context()->getSettingsRef())
    , is_snapshot_mode(is_snapshot_mode_)
    , is_clickhouse_compatible_mode(is_clickhouse_compatible_mode_)
    , log(getLogger(name))
{
}

/// FIXME : Get the corresponding database according to the user name
String RestHTTPRequestHandler::getDatabaseByUser(const String & user) const
{
    String database = "default";

    if (user == "system")
        return "system";

    if (user == "neutron")
        return "neutron";

    if (database.empty())
    {
        throw Exception(
            ErrorCodes::UNKNOWN_DATABASE, "Unknown database: Cannot find database information corresponding to user '{}'", user);
    }

    return database;
}

void RestHTTPRequestHandler::trySendExceptionToClient(
    const String & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        response.set("x-timeplus-exception-code", std::to_string(exception_code));

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
            response.requireAuthentication("proton server HTTP API");
        }
        else
        {
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
        }

        if (!response.sent())
        {
            /// If nothing was sent yet and we don't even know if we must compress the response.
            *response.send() << s << "\n";
        }
        else
        {
            LOG_ERROR(log, "Failed to send response: {}, code={}", s, exception_code);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}
}
