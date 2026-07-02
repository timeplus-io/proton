#include <Server/HTTP/authenticateUserByHTTP.h>

#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Common/Base64.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>

#include <Poco/Net/HTTPBasicCredentials.h>

#if USE_SSL
#include <Poco/Net/X509Certificate.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{
    /// Throws an exception that multiple authorization schemes are used simultaneously.
    [[noreturn]] void throwMultipleAuthenticationMethods(std::string_view method1, std::string_view method2)
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "Invalid authentication: it is not allowed to use {} and {} simultaneously", method1, method2);
    }

    /// Checks that a specified user name is not empty, and throws an exception if it's empty.
    void checkUserNameNotEmpty(const String & user_name, std::string_view method)
    {
        if (user_name.empty())
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Got an empty user name from {}", method);
    }
}


bool authenticateUserByHTTP(
    const HTTPServerRequest & request,
    const HTMLForm & params,
    HTTPServerResponse & response,
    Session & session,
    std::unique_ptr<Credentials> & request_credentials,
    ContextPtr global_context,
    LoggerPtr log)
{
    /// Get the credentials created by the previous call of authenticateUserByHTTP() while handling the previous HTTP request.
    auto current_credentials = std::move(request_credentials);

    /// proton: starts
    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("x-timeplus-user", request.get("x-proton-user", ""));
    std::string password = request.get("x-timeplus-key", request.get("x-proton-key", ""));
    std::string quota_key = request.get("x-timeplus-quota", request.get("x-proton-quota", ""));
    /// proton: ends

    std::string spnego_challenge;
    
    /// User name and password can be passed using HTTP Basic auth or query parameters
    /// (both methods are insecure).
    bool has_http_credentials = request.hasCredentials();
    bool has_credentials_in_query_params = params.has("user") || params.has("password");


    if (has_http_credentials)
    {
        /// It is prohibited to mix different authorization schemes.
        if (has_credentials_in_query_params)
            throwMultipleAuthenticationMethods("Authorization HTTP header", "authentication via parameters");

        std::string scheme;
        std::string auth_info;
        request.getCredentials(scheme, auth_info);

        if (Poco::icompare(scheme, "Basic") == 0)
        {
            Poco::Net::HTTPBasicCredentials credentials(auth_info);
            user = credentials.getUsername();
            password = credentials.getPassword();
            checkUserNameNotEmpty(user, "Authorization HTTP header");
        }
        else if (Poco::icompare(scheme, "Negotiate") == 0)
        {
            spnego_challenge = auth_info;

            if (spnego_challenge.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: SPNEGO challenge is empty");
        }
        else
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: '{}' HTTP Authorization scheme is not supported", scheme);
        }
    }
    else
    {
        /// If the user name is not set we assume it's the 'default' user.
        user = params.get("user", "default");
        password = params.get("password", "");
        checkUserNameNotEmpty(user, "authentication via parameters");
    }

    if (!spnego_challenge.empty())
    {
        if (!current_credentials)
            current_credentials = global_context->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(current_credentials.get());
        if (!gss_acceptor_context)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma clang diagnostic pop

        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: 'Negotiate' HTTP Authorization failure");

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
            request_credentials = std::move(current_credentials);
            return false;
        }
    }
    else // I.e., now using user name and password strings ("Basic").
    {
        if (!current_credentials)
            current_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(current_credentials.get());
        if (!basic_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected 'Basic' HTTP Authorization scheme");

        chassert(!user.empty());
        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }

    if (params.has("quota_key"))
    {
        if (!quota_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid authentication: it is not allowed "
                            "to use quota key as HTTP header and as parameter simultaneously");

        quota_key = params.get("quota_key");
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.
    /// proton: starts
    /// session.setHTTPClientInfo(request);
    /// session.setQuotaClientKey(quota_key);
    ClientInfo & client_info = session.getClientInfo();

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");
    client_info.quota_key = quota_key;
    /// proton: ends

    try
    {
            session.authenticate(*current_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        current_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        current_credentials = global_context->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }

    return true;
}

}
