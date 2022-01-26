#include "MetaStoreHandler.h"

#if USE_NURAFT

#    include <Coordination/KVRequest.h>
#    include <Coordination/MetaStoreDispatcher.h>
#    include <Coordination/ReadBufferFromNuraftBuffer.h>
#    include <DistributedMetadata/CatalogService.h>
#    include <DistributedMetadata/sendRequest.h>

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
    const String METASTORE_URL = "http://{}:{}/proton/metastore{}";

    String buildResponse(const String & query_id)
    {
        Poco::JSON::Object resp;
        resp.set("request_id", query_id);

        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }

    String buildResponse(const String & namespace_, const std::vector<String> & keys, const std::vector<String> & values, const String & query_id)
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

    String buildResponse(const String & namespace_, const std::vector<std::pair<String, String> > & kv_pairs, const String & query_id)
    {
        Poco::JSON::Object resp;
        resp.set("request_id", query_id);

        Poco::JSON::Object data;
        for (auto & [key, value] : kv_pairs)
            data.set(key, value);

        resp.set("data", data);

        if (!namespace_.empty())
            resp.set("namespace", namespace_);

        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }

    std::vector<std::pair<String, String> > parseRequestPairsFromJSONObject(const Poco::JSON::Object::Ptr & object)
    {
        std::vector<std::pair<String, String> > pairs;
        for (const auto & [key, value] : *object)
            pairs.emplace_back(key, value.extract<String>());

        return pairs;
    }
}

std::pair<String, Int32> MetaStoreHandler::executeGet(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        /// So far, only support single key
        auto request_key = getPathParameter("key", getQueryParameter("key"));
        auto request_prefix = getQueryParameter("key_prefix");

        if (!request_key.empty())
        {
            /// get by 'key'
            if (!request_prefix.empty())
                return {jsonErrorResponse("Invalid get request: conflicting parameters 'key' and 'key_prefix'.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

            return doGet(payload, {request_key});
        }
        else
        {
            /// list by 'key_prefix'
            return doList(payload, request_prefix);
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

std::pair<String, Int32> MetaStoreHandler::doGet(const Poco::JSON::Object::Ptr & payload, const Strings & request_keys) const
{
    assert(!request_keys.empty());

    if (!metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    auto namespace_ = getPathParameter("namespace");

    /// If true, execute read requests as writes through whole RAFT consesus with similar speed
    bool enable_quorum_reads = getQueryParameterBool("quorum_reads", metastore_dispatcher->getSettings()->quorum_reads);

    if (metastore_dispatcher->isLeader() && !enable_quorum_reads)
    {
        std::vector<String> values = metastore_dispatcher->localMultiGetByKeys(request_keys, namespace_);
        return {buildResponse(namespace_, request_keys, values, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    else if (metastore_dispatcher->isLeader() || (metastore_dispatcher->isSupportAutoForward() && enable_quorum_reads))
    {
        auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIGET);
        request->as<Coordination::KVMultiGetRequest>()->keys = request_keys;

        auto response = metastore_dispatcher->putRequest(request, namespace_);
        if (response->code != ErrorCodes::OK)
            return {
                jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                HTTPResponse::HTTP_BAD_REQUEST};

        auto resp = response->as<const Coordination::KVMultiGetResponse>();
        return {buildResponse(namespace_, resp->kv_pairs, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    else
    {
        /// Forward request to leader node manually
        return forwardRequest(payload, fmt::format("?quorum_reads={}", enable_quorum_reads));
    }
}


std::pair<String, Int32> MetaStoreHandler::doList(const Poco::JSON::Object::Ptr & payload, const String & request_prefix) const
{
    if (!metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    auto namespace_ = getPathParameter("namespace");

    /// If true, execute read requests as writes through whole RAFT consesus with similar speed
    bool enable_quorum_reads = getQueryParameterBool("quorum_reads", metastore_dispatcher->getSettings()->quorum_reads);

    if (metastore_dispatcher->isLeader() && !enable_quorum_reads)
    {
        auto kv_pairs = metastore_dispatcher->localRangeGetByNamespace(request_prefix, namespace_);
        return {buildResponse(namespace_, kv_pairs, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    else if (metastore_dispatcher->isLeader() || (metastore_dispatcher->isSupportAutoForward() && enable_quorum_reads))
    {
        auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::LIST);
        request->as<Coordination::KVListRequest>()->key_prefix = request_prefix;

        auto response = metastore_dispatcher->putRequest(request, namespace_);
        if (response->code != ErrorCodes::OK)
            return {
                jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                HTTPResponse::HTTP_BAD_REQUEST};

        auto resp = response->as<const Coordination::KVListResponse>();
        return {buildResponse(namespace_, resp->kv_pairs, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    else
    {
        /// Forward request to leader node manually
        return forwardRequest(payload);
    }
}

std::pair<String, Int32> MetaStoreHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        const auto & request_pairs = parseRequestPairsFromJSONObject(payload);
        if (request_pairs.empty())
            return {jsonErrorResponse("Invalid request data: expected map of key-value(s).", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        if (!metastore_dispatcher->hasLeader())
            return {
                jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

        auto namespace_ = getPathParameter("namespace");

        if (metastore_dispatcher->isLeader() || metastore_dispatcher->isSupportAutoForward())
        {
            auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIPUT);
            request->as<Coordination::KVMultiPutRequest>()->kv_pairs = request_pairs;

            auto response = metastore_dispatcher->putRequest(request, namespace_);
            if (response->code != ErrorCodes::OK)
                return {
                    jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                    HTTPResponse::HTTP_BAD_REQUEST};

            return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else
        {
            /// Forward request to leader node manually
            return forwardRequest(payload);
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

std::pair<String, Int32> MetaStoreHandler::executeDelete(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        /// So far, only support single key
        auto request_key = getPathParameter("key", getQueryParameter("key"));
        if (request_key.empty())
            return {jsonErrorResponse("Invalid request data: expected delete-key.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        if (!metastore_dispatcher->hasLeader())
            return {
                jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

        auto namespace_ = getPathParameter("namespace");

        if (metastore_dispatcher->isLeader() || metastore_dispatcher->isSupportAutoForward())
        {
            auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIDELETE);
            request->as<Coordination::KVMultiDeleteRequest>()->keys = {request_key};

            auto response = metastore_dispatcher->putRequest(request, namespace_);
            if (response->code != ErrorCodes::OK)
                return {
                    jsonErrorResponse(fmt::format("Request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                    HTTPResponse::HTTP_BAD_REQUEST};

            return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else
        {
            /// Forward request to leader node manually
            return forwardRequest(payload);
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

std::pair<String, Int32> MetaStoreHandler::forwardRequest(const Poco::JSON::Object::Ptr & payload, const String & uri_parameter) const
{
    Poco::URI uri{fmt::format(
        METASTORE_URL,
        metastore_dispatcher->getLeaderHostname(),
        metastore_dispatcher->getHttpPort(),
        uri_parameter)};
    std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    payload->stringify(req_body_stream, 0);
    const String & body = req_body_stream.str();

    auto [response, http_status] = sendRequest(
        uri,
        HTTPRequest::HTTP_POST,
        query_context->getCurrentQueryId(),
        query_context->getUserName(),
        query_context->getPasswordByUserName(query_context->getUserName()),
        body,
        log);
    if (http_status == HTTPResponse::HTTP_OK)
    {
        return {response, http_status};
    }
    return {jsonErrorResponseFrom(response), http_status};
}

}
#endif
