#include "UDFHandler.h"
#include "SchemaValidator.h"

#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Interpreters/Streaming/ASTToJSONUtils.h>
#include <Interpreters/Streaming/MetaStoreJSONConfigRepository.h>
#include <V8/Utils.h>

#include <numeric>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UDF_INVALID_NAME;
}

namespace
{
String buildResponse(const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const Poco::JSON::Array::Ptr & object, const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);
    resp.set("data", object);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const Poco::JSON::Object::Ptr & object, const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);
    resp.set("data", object);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}

std::pair<String, Int32> UDFHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & func = getPathParameter("func");
    if (func.empty())
    {
        /// List all functions
        Poco::JSON::Array::Ptr functions = metastore_repo->list();
        return {buildResponse(functions, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }

    /// Get by function name
    Poco::JSON::Object::Ptr object = metastore_repo->get(func);
    Poco::JSON::Object::Ptr def = object->getObject("function");
    return {buildResponse(def, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::map<String, std::map<String, String>> UDFHandler::create_schema
    = {{"required", {{"name", "string"}, {"type", "string"}, {"return_type", "string"}}},
       {"optional", {{"format", "string"}, {"command", "string"}, {"url", "string"}, {"auth_method", "string"}}}};

bool UDFHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(create_schema, payload, error_msg);
}

std::pair<String, Int32> UDFHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    String name = payload->get("name").toString();
    Poco::JSON::Object::Ptr func(new Poco::JSON::Object());
    func->set("function", payload);
    UserDefinedFunctionFactory::instance().registerFunction(
        query_context, name, func, true /* throw_if_exists*/, true /*replace_if_exists*/);
    return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> UDFHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & func = getPathParameter("func", "");
    if (func.empty())
        return {jsonErrorResponse("Missing UDF name", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    UserDefinedFunctionFactory::instance().unregisterFunction(query_context, func, true /*throw_if_not_exists*/);
    return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}
}
