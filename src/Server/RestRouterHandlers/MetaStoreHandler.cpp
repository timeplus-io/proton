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
    const String METASTORE_URL = "http://{}:{}/proton/metastore";

    String buildResponse(const String & query_id)
    {
        Poco::JSON::Object resp;
        resp.set("request_id", query_id);

        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }

    String buildResponse(const std::vector<String> & keys, const std::vector<String> & values, const String & query_id)
    {
        Poco::JSON::Object resp;
        resp.set("request_id", query_id);

        Poco::JSON::Array array_keys;
        for (const auto & key : keys)
            array_keys.add(key);
        resp.set("keys", array_keys);

        Poco::JSON::Array array_values;
        for (const auto & value : values)
            array_values.add(value);
        resp.set("values", array_values);

        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }

    String parseStringFromJSONObject(const Poco::JSON::Object::Ptr & object, const String & key)
    {
        if (object && object->has(key))
            return object->getValue<String>(key);
        else
            return {};
    }

    std::vector<String> parseStringsFromJSONObject(const Poco::JSON::Object::Ptr & object, const String & key)
    {
        std::vector<String> values;
        if (object && object->has(key))
        {
            auto array = object->getArray(key);
            for (size_t i = 0; i < array->size(); ++i)
            {
                auto value = array->getElement<String>(i);
                if (!value.empty())
                    values.emplace_back(value);
            }
        }
        return values;
    }
}

std::pair<String, Int32> MetaStoreHandler::executeGet(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        /// we get key from payload
        std::vector<String> request_keys = parseStringsFromJSONObject(payload, "keys");
        auto key = parseStringFromJSONObject(payload, "key");
        if (!key.empty())
            request_keys.emplace_back(key);

        if (request_keys.empty())
            return {jsonErrorResponse("Here is not 'key' or 'keys'.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        if (!metastore_dispatcher->hasLeader())
            return {
                jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER),
                HTTPResponse::HTTP_BAD_REQUEST};

        /// If true, execute read requests as writes through whole RAFT consesus with similar speed
        bool enable_quorum_reads = getQueryParameterBool("quorum_reads", metastore_dispatcher->getSettings()->quorum_reads);

        if (metastore_dispatcher->isLeader() && !enable_quorum_reads)
        {
            std::vector<String> values = metastore_dispatcher->localMultiGetByKeys(request_keys);
            return {buildResponse(request_keys, values, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else if (metastore_dispatcher->isLeader() || (metastore_dispatcher->isSupportAutoForward() && enable_quorum_reads))
        {
            auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIGET);
            request->as<Coordination::KVMultiGetRequest>()->keys = request_keys;

            auto response = metastore_dispatcher->putRequest(request);
            if (response->code != ErrorCodes::OK)
                return {
                    jsonErrorResponse(fmt::format("request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                    HTTPResponse::HTTP_BAD_REQUEST};

            auto resp = response->as<const Coordination::KVMultiGetResponse>();
            return {buildResponse(request_keys, resp->values, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else
        {
            /// Forward request to leader node manually
            Poco::URI uri{fmt::format(
                METASTORE_URL,
                metastore_dispatcher->getLeaderHostname(),
                metastore_dispatcher->getHttpPort(),
                fmt::format("?quorum_reads={}", enable_quorum_reads))};
            std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
            payload->stringify(req_body_stream, 0);
            const String & body = req_body_stream.str();

            auto [response, http_status] = sendRequest(
                uri,
                HTTPRequest::HTTP_GET,
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
    catch (std::exception & e)
    {
        return {jsonErrorResponse(fmt::format("request error: {}", e.what()), ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
    catch (...)
    {
        return {jsonErrorResponse("request unknown error.", ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

std::pair<String, Int32> MetaStoreHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    /// we get key/value from payload
    std::vector<String> request_keys = parseStringsFromJSONObject(payload, "keys");
    auto key = parseStringFromJSONObject(payload, "key");
    if (!key.empty())
        request_keys.emplace_back(key);

    std::vector<String> request_values = parseStringsFromJSONObject(payload, "values");
    auto value = parseStringFromJSONObject(payload, "value");
    if (!value.empty())
        request_values.emplace_back(value);

    if (request_keys.empty())
        return {jsonErrorResponse("Here is not 'key' or 'keys'.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    if (request_values.empty())
        return {jsonErrorResponse("Here is not 'value' or 'values'.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    if (request_keys.size() != request_values.size())
        return {
            jsonErrorResponse("Here is not matched size of keys and values.", ErrorCodes::BAD_REQUEST_PARAMETER),
            HTTPResponse::HTTP_BAD_REQUEST};

    if (!metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    try
    {
        if (metastore_dispatcher->isLeader() || metastore_dispatcher->isSupportAutoForward())
        {
            auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIPUT);
            request->as<Coordination::KVMultiPutRequest>()->keys = request_keys;
            request->as<Coordination::KVMultiPutRequest>()->values = request_values;

            auto response = metastore_dispatcher->putRequest(request);
            if (response->code != ErrorCodes::OK)
                return {
                    jsonErrorResponse(fmt::format("request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                    HTTPResponse::HTTP_BAD_REQUEST};

            return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else
        {
            /// Forward request to leader node manually
            Poco::URI uri{fmt::format(
                METASTORE_URL,
                metastore_dispatcher->getLeaderHostname(),
                metastore_dispatcher->getHttpPort(),
                /* no parameters */ "")};
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
    catch (std::exception & e)
    {
        return {jsonErrorResponse(fmt::format("request error: {}", e.what()), ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
    catch (...)
    {
        return {jsonErrorResponse("request unknown error.", ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

std::pair<String, Int32> MetaStoreHandler::executeDelete(const Poco::JSON::Object::Ptr & payload) const
{
    /// we get key from payload
    std::vector<String> request_keys = parseStringsFromJSONObject(payload, "keys");
    auto key = parseStringFromJSONObject(payload, "key");
    if (!key.empty())
        request_keys.emplace_back(key);

    if (request_keys.empty())
        return {jsonErrorResponse("Here is not 'key' or 'keys'.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    if (!metastore_dispatcher->hasLeader())
        return {
            jsonErrorResponse("Ignoring request, because no alive leader exist", ErrorCodes::NOT_A_LEADER), HTTPResponse::HTTP_BAD_REQUEST};

    try
    {
        if (metastore_dispatcher->isLeader() || metastore_dispatcher->isSupportAutoForward())
        {
            auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIDELETE);
            request->as<Coordination::KVMultiDeleteRequest>()->keys = request_keys;

            auto response = metastore_dispatcher->putRequest(request);
            if (response->code != ErrorCodes::OK)
                return {
                    jsonErrorResponse(fmt::format("request failed[{}]: {}", response->code, response->msg), ErrorCodes::LOGICAL_ERROR),
                    HTTPResponse::HTTP_BAD_REQUEST};

            return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
        }
        else
        {
            /// Forward request to leader node manually
            Poco::URI uri{fmt::format(
                METASTORE_URL,
                metastore_dispatcher->getLeaderHostname(),
                metastore_dispatcher->getHttpPort(),
                /* no parameters */ "")};
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
    catch (std::exception & e)
    {
        return {jsonErrorResponse(fmt::format("request error: {}", e.what()), ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
    catch (...)
    {
        return {jsonErrorResponse("request unknown error.", ErrorCodes::LOGICAL_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

}

#endif
