#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>
#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int UNKNOWN_EXCEPTION;
}

class RestRouterHandler : private boost::noncopyable
{
public:
    RestRouterHandler(ContextPtr query_context_, const String & router_name)
        : query_context(query_context_), log(&Poco::Logger::get(router_name))
    {
        database = query_context->getCurrentDatabase();
        assert(!database.empty());
    }
    virtual ~RestRouterHandler() = default;

    /// Execute request and return response in `String`. If it failed
    /// a correct `http_status` code will be set by trying best.
    /// This function may throw, and caller will need catch the exception
    /// and sends back HTTP `500` to clients
    void execute(HTTPServerRequest & request, HTTPServerResponse & response);

    String getPathParameter(const String & name, const String & default_value = "") const
    {
        auto iter = path_parameters.find(name);
        if (iter != path_parameters.end())
        {
            return iter->second;
        }
        else
        {
            return default_value;
        }
    }

    void setupDistributedQueryParameters(const std::map<String, String> & parameters, const Poco::JSON::Object::Ptr & payload = nullptr) const;


    inline bool isDistributedDDL() const { return query_context->isDistributed() && getQueryParameterBool("distributed_ddl", true); }

    void setPathParameter(const String & name, const String & value) { path_parameters[name] = value; }

    String getQueryParameter(const String & name, const String & default_value = "") const
    {
        return query_parameters->get(name, default_value);
    }

    bool getQueryParameterBool(const String & name, bool default_value = false) const
    {
        const auto & val = query_parameters->get(name, "");
        if (val.empty())
        {
            return default_value;
        }

        if (val == "false" || val == "0")
        {
            return false;
        }
        return true;
    }

    const String & getAcceptEncoding() const { return accepted_encoding; }

    const String & getContentEncoding() const { return content_encoding; }

    Int64 getContentLength() const { return content_length; }

    bool hasQueryParameter(const String & name) const { return query_parameters->has(name); }

public:
    static String jsonErrorResponse(const String & error_msg, int error_code, const String & query_id)
    {
        std::stringstream error_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Object error_resp;

        error_resp.set("error_msg", error_msg);
        error_resp.set("code", error_code);
        error_resp.set("request_id", query_id);
        error_resp.stringify(error_str_stream, 0);
        return error_str_stream.str();
    }

protected:
    String jsonErrorResponse(const String & error_msg, int error_code) const
    {
        return jsonErrorResponse(error_msg, error_code, query_context->getCurrentQueryId());
    }

    /// Compose the error response from a response of a forwared request
    String jsonErrorResponseFrom(const String & response, int error_code = ErrorCodes::UNKNOWN_EXCEPTION) const
    {
        if (response.find("request_id") != String::npos && response.find("error_msg") != String::npos)
        {
            /// It is already a well-formed response in JSON from remote
            return response;
        }
        return jsonErrorResponse("Internal server error", error_code);
    }

    String processQuery(
        const String & query, const std::function<void(Block &&)> & callback = [](Block &&) {}) const;

    String processQuery(const String & query, Poco::JSON::Object & resp, const std::function<void(Block &&)> & callback) const;

private:
    /// Override this function if derived handler need write data in a streaming way to http output
    virtual bool streamingOutput() const { return false; }

    /// Override this function if derived handler need read data in a streaming way from http input
    virtual bool streamingInput() const { return false; }

    /// Handle the request in streaming way, so far Ingest API probably needs override this function
    virtual std::pair<String, Int32> execute(ReadBuffer & /* input */) const { return handleNotImplemented(); }

    /// Sending response in a streaming way
    virtual void execute(const Poco::JSON::Object::Ptr & /* payload */, HTTPServerResponse & response) const
    {
        auto result{handleNotImplemented()};
        response.setStatusAndReason(HTTPResponse::HTTPStatus(result.second));
        *response.send() << result.first << std::endl;
    }

    std::pair<String, Int32> handleNotImplemented() const
    {
        return {
            jsonErrorResponse("HTTP method requested is not supported", ErrorCodes::UNKNOWN_TYPE_OF_QUERY),
            HTTPResponse::HTTP_NOT_IMPLEMENTED};
    }

    std::pair<String, Int32> execute(const Poco::JSON::Object::Ptr & payload) const
    {
        const auto & client_info = query_context->getClientInfo();

        if (client_info.http_method == ClientInfo::HTTPMethod::GET)
        {
            return doExecute(&RestRouterHandler::validateGet, &RestRouterHandler::executeGet, payload);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::POST)
        {
            return doExecute(&RestRouterHandler::validatePost, &RestRouterHandler::executePost, payload);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::PATCH)
        {
            return doExecute(&RestRouterHandler::validatePatch, &RestRouterHandler::executePatch, payload);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::DELETE)
        {
            return doExecute(&RestRouterHandler::validateDelete, &RestRouterHandler::executeDelete, payload);
        }

        return handleNotImplemented();
    }

    template <typename Validate, typename Execute>
    std::pair<String, Int32> doExecute(Validate validate, Execute exec, const Poco::JSON::Object::Ptr & payload) const
    {
        String error_msg;
        if (!(this->*validate)(payload, error_msg))
        {
            return {jsonErrorResponse(error_msg, ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};
        }
        return (this->*exec)(payload);
    }

    /// Return http response payload and http code
    virtual std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
    {
        return handleNotImplemented();
    }

    /// Return http response payload and http code
    virtual std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & /* payload */) const
    {
        return handleNotImplemented();
    }

    virtual std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const { return handleNotImplemented(); }

    virtual std::pair<String, Int32> executePatch(const Poco::JSON::Object::Ptr & /* payload */) const { return handleNotImplemented(); }

    virtual bool validateGet(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validatePost(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validateDelete(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validatePatch(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }

    void setupHTTPContext(const HTTPServerRequest & request)
    {
        accepted_encoding = request.get("Accept-Encoding", "");
        content_encoding = request.get("Content-Encoding", "");
        content_length = request.getContentLength64();
        setupQueryParams(request);

        if (isDistributedDDL())
        {
            setupRawQuery(request);
        }
    }

    void setupQueryParams(const HTTPServerRequest & request) { query_parameters = std::make_unique<HTMLForm>(request); }

    void setupRawQuery(const HTTPServerRequest & request)
    {
        Poco::URI uri(request.getURI());
        query_context->setQueryParameter("url_parameters", uri.getRawQuery());
    }

protected:
    ContextPtr query_context;
    Poco::Logger * log;

    std::unordered_map<String, String> path_parameters;
    std::unique_ptr<HTMLForm> query_parameters;
    String accepted_encoding;
    String content_encoding;
    Int64 content_length;
    String database;
};

using RestRouterHandlerPtr = std::shared_ptr<RestRouterHandler>;

}
