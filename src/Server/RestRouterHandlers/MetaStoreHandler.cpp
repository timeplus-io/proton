#include "MetaStoreHandler.h"

#if USE_NURAFT

#    include <Coordination/KVRequest.h>
#    include <Coordination/MetaStoreDispatcher.h>
#    include <Coordination/ReadBufferFromNuraftBuffer.h>
#    include <Common/sendRequest.h>

#    include <Poco/Path.h>

#    include <numeric>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int BAD_REQUEST_PARAMETER;
    extern const int NOT_A_LEADER;
    extern const int TIMEOUT_EXCEEDED;
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

String
buildResponse(const String & namespace_, const std::vector<String> & keys, const std::vector<String> & values, const String & query_id)
{
    assert(keys.size() == values.size());

    Poco::JSON::Object resp;
    resp.set("request_id", query_id);

    Poco::JSON::Object data;
    for (size_t i = 0; i < values.size(); ++i)
        data.set(keys[i], values[i]);

    resp.set("data", data);

    if (!namespace_.empty())
        resp.set("namespace", namespace_);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const String & namespace_, const std::vector<std::pair<String, String>> & kv_pairs, const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);

    Poco::JSON::Object data;
    for (const auto & [key, value] : kv_pairs)
        data.set(key, value);

    resp.set("data", data);

    if (!namespace_.empty())
        resp.set("namespace", namespace_);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

std::vector<std::pair<String, String>> parseRequestPairsFromJSONObject(const Poco::JSON::Object::Ptr & object)
{
    std::vector<std::pair<String, String>> pairs;
    for (const auto & [key, value] : *object)
        pairs.emplace_back(key, value.extract<String>());

    return pairs;
}

std::pair<String, String> parseRequestNamespaceAndKeyFromPath(const String & path)
{
    /// PATH: '/proton/metastore/{namespace}[/{key}] ...'
    constexpr char prefix[] = "/proton/metastore/";
    assert(path.starts_with(prefix));
    size_t root_len = strlen(prefix);
    auto namespace_end = path.find_first_of('/', root_len);
    if (namespace_end == std::string::npos)
        return {path.substr(root_len), ""};
    else
        return {String(path, root_len, namespace_end - root_len), path.substr(namespace_end + 1)};
}
}

std::pair<String, Int32> MetaStoreHandler::executeGet(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        /// So far, only support single key
        auto [request_namespace, request_key] = parseRequestNamespaceAndKeyFromPath(this->query_uri.getPath());
        if (request_key.empty())
            request_key = getQueryParameter("key");

        auto request_prefix = getQueryParameter("key_prefix");

        if (!request_key.empty())
        {
            /// get by 'key'
            if (!request_prefix.empty())
                return {
                    jsonErrorResponse(
                        "Invalid get request: conflicting parameters 'key' and 'key_prefix'.", ErrorCodes::BAD_REQUEST_PARAMETER),
                    HTTPResponse::HTTP_BAD_REQUEST};

            return doGet(payload, request_namespace, {request_key});
        }
        else
        {
            /// list by 'key_prefix'
            return doList(payload, request_namespace, request_prefix);
        }
    }
    catch (...)
    {
        auto exception = std::current_exception();
        return {
            jsonErrorResponse("Request failed: " + getExceptionMessage(exception, false), getExceptionErrorCode(exception)),
            HTTPResponse::HTTP_BAD_REQUEST};
    }
}

std::pair<String, Int32>
MetaStoreHandler::doGet(const Poco::JSON::Object::Ptr & /*payload*/, const String & namespace_, const Strings & request_keys) const
{
    assert(!request_keys.empty());

    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    std::vector<String> values = metastore_dispatcher->localMultiGetByKeys(request_keys, namespace_);
    return {buildResponse(namespace_, request_keys, values, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32>
MetaStoreHandler::doList(const Poco::JSON::Object::Ptr & /*payload*/, const String & namespace_, const String & request_prefix) const
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    auto kv_pairs = metastore_dispatcher->localRangeGetByNamespace(request_prefix, namespace_);
    return {buildResponse(namespace_, kv_pairs, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> MetaStoreHandler::doMultiGet(const Poco::JSON::Object::Ptr & payload, const String & namespace_) const
{
    std::vector<String> request_keys;
    auto json_arguments = payload->getArray("keys");
    for (unsigned int i = 0; i < json_arguments->size(); i++)
        request_keys.emplace_back(json_arguments->get(i).toString());

    return doGet(payload, namespace_, request_keys);
}

std::pair<String, Int32>
MetaStoreHandler::doDelete(const Poco::JSON::Object::Ptr & /*payload*/, const String & namespace_, const std::vector<String> & keys) const
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};


    auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIDELETE);
    request->as<Coordination::KVMultiDeleteRequest>()->keys = keys;

    auto response = metastore_dispatcher->putRequest(request, namespace_);
    if (response->code != ErrorCodes::OK)
        return {
            jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
            HTTPResponse::HTTP_BAD_REQUEST};

    return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> MetaStoreHandler::doMultiDelete(const Poco::JSON::Object::Ptr & payload, const String & namespace_) const
{
    std::vector<String> request_keys;
    const auto json_arguments = payload->getArray("keys");
    for (unsigned int i = 0; i < json_arguments->size(); i++)
        request_keys.emplace_back(json_arguments->get(i).toString());

    return doDelete(payload, namespace_, request_keys);
}

std::pair<String, Int32> MetaStoreHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        auto [namespace_, key] = parseRequestNamespaceAndKeyFromPath(this->query_uri.getPath());
        const auto action = getQueryParameter("action", "");
        if (action == "multiget")
            return doMultiGet(payload, namespace_);
        else if (action == "multidelete")
            return doMultiDelete(payload, namespace_);

        const auto & request_pairs = parseRequestPairsFromJSONObject(payload);
        if (request_pairs.empty())
            return {
                jsonErrorResponse("Invalid request data: expected map of key-value(s).", ErrorCodes::BAD_REQUEST_PARAMETER),
                HTTPResponse::HTTP_BAD_REQUEST};

        if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
            return {
                jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER),
                HTTPResponse::HTTP_BAD_REQUEST};

        auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIPUT);
        request->as<Coordination::KVMultiPutRequest>()->kv_pairs = request_pairs;

        auto response = metastore_dispatcher->putRequest(request, namespace_);
        if (response->code != ErrorCodes::OK)
            return {
                jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                HTTPResponse::HTTP_BAD_REQUEST};

        return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (...)
    {
        auto exception = std::current_exception();
        return {
            jsonErrorResponse("Request failed: " + getExceptionMessage(exception, false), getExceptionErrorCode(exception)),
            HTTPResponse::HTTP_BAD_REQUEST};
    }
}

std::pair<String, Int32> MetaStoreHandler::executeDelete(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        /// So far, only support single key
        auto [namespace_, request_key] = parseRequestNamespaceAndKeyFromPath(this->query_uri.getPath());
        if (request_key.empty())
            request_key = getQueryParameter("key");

        if (request_key.empty())
            return {
                jsonErrorResponse("Invalid request data: expected delete-key.", ErrorCodes::BAD_REQUEST_PARAMETER),
                HTTPResponse::HTTP_BAD_REQUEST};

        return doDelete(payload, namespace_, {request_key});
    }
    catch (...)
    {
        auto exception = std::current_exception();
        return {
            jsonErrorResponse("Request failed: " + getExceptionMessage(exception, false), getExceptionErrorCode(exception)),
            HTTPResponse::HTTP_BAD_REQUEST};
    }
}
}
#endif
