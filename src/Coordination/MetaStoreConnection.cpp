#include "MetaStoreConnection.h"

#include <Common/ProtonCommon.h>
#include <Common/sendRequest.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/setThreadName.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int ROCKSDB_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
Poco::JSON::Object::Ptr buildJSONStringArray(const String & name, const std::vector<String> & values)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    Poco::JSON::Array array;
    for (const auto & v : values)
        array.add({v});
    json->set(name, array);

    return json;
}
}

MetaStoreConnection::MetaStoreConnection(const Poco::Util::AbstractConfiguration & config) : log(&Poco::Logger::get("MetaStoreDispatcher"))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("metastore_server.raft_configuration", keys);
    const auto rest_http_port = config.getString("http_port");
    startup_timeout = config.getInt("metastore_server.coordination_settings.startup_timeout", 60000);

    for (const auto & server_key : keys)
    {
        if (!startsWith(server_key, "server"))
            continue;

        std::string full_prefix = "metastore_server.raft_configuration." + server_key;
        active_servers.emplace_back(
            fmt::format("http://{}:{}", config.getString(full_prefix + ".hostname"), config.getString(full_prefix + ".http_port")));
    }
}

String MetaStoreConnection::localGetByKey(const String & key, const String & namespace_) const
{
    auto [resp, status] = forwardRequest(
        Poco::Net::HTTPRequest::HTTP_GET, "internal-query-id", "default", "", nullptr, fmt::format("/{}/{}", namespace_, key));

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get '{}' error: {}", key, resp);
    Poco::JSON::Parser parser;
    auto json = parser.parse(resp).extract<Poco::JSON::Object::Ptr>();
    auto data = json->getObject("data");
    if (!data || !data->has(key))
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get '{}' error: {}", key, resp);

    return data->get(key).toString();
}

std::vector<String> MetaStoreConnection::localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const
{
    Poco::JSON::Object::Ptr json = buildJSONStringArray("keys", keys);

    auto [resp, status] = forwardRequest(
        Poco::Net::HTTPRequest::HTTP_POST, "internal-query-id", "default", "", json, fmt::format("/{}?action=multiget", namespace_));

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
        throw Exception(ErrorCodes::ROCKSDB_ERROR, resp);

    std::vector<String> values;
    Poco::JSON::Parser parser;
    auto json_resp = parser.parse(resp).extract<Poco::JSON::Object::Ptr>();
    auto data = json_resp->getObject("data");
    if (unlikely(!data))
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get keys");

    for (const auto & [_, value] : *data)
        values.emplace_back(value.extract<String>());

    return values;
}

std::vector<std::pair<String, String>>
MetaStoreConnection::localRangeGetByNamespace(const String & prefix_, const String & namespace_) const
{
    std::vector<std::pair<String, String>> result;

    auto [resp, status] = forwardRequest(
        Poco::Net::HTTPRequest::HTTP_GET,
        "internal-query-id",
        "default",
        "",
        nullptr,
        fmt::format("/{}?key_prefix={}", namespace_, prefix_));

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get keys with prefix, error: {}", prefix_, resp);

    Poco::JSON::Parser parser;
    auto json = parser.parse(resp).extract<Poco::JSON::Object::Ptr>();
    auto data = json->getObject("data");
    if (data)
    {
        for (const auto & [key, value] : *data)
            result.emplace_back(key, value.extract<String>());
    }

    return result;
}

Coordination::KVResponsePtr MetaStoreConnection::putRequest(const Coordination::KVRequestPtr & request, const String & namespace_)
{
    const auto & op = request->getOpNum();
    auto response = Coordination::KVResponseFactory::instance().get(request);

    switch (op)
    {
        case Coordination::KVOpNum::MULTIPUT: {
            const auto * req = request->as<const Coordination::KVMultiPutRequest>();
            Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
            for (const auto & [k, v] : req->kv_pairs)
                json->set(k, v);

            auto [resp, status] = forwardRequest(
                Poco::Net::HTTPRequest::HTTP_POST, "internal-query-id", "default", "", json, fmt::format("/{}", namespace_));

            if (status != Poco::Net::HTTPResponse::HTTP_OK)
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", resp);

            break;
        }
        case Coordination::KVOpNum::MULTIDELETE: {
            const auto * req = request->as<const Coordination::KVMultiDeleteRequest>();
            Poco::JSON::Object::Ptr json = buildJSONStringArray("keys", req->keys);
            auto [resp, status] = forwardRequest(
                Poco::Net::HTTPRequest::HTTP_POST,
                "internal-query-id",
                "default",
                "",
                json,
                fmt::format("/{}?action=multidelete", namespace_));
            if (status != Poco::Net::HTTPResponse::HTTP_OK)
                throw Exception(ErrorCodes::ROCKSDB_ERROR, resp);
            break;
        }
        case Coordination::KVOpNum::LIST: {
            const auto * req = request->as<const Coordination::KVListRequest>();
            auto * resp = response->as<Coordination::KVListResponse>();
            resp->kv_pairs = localRangeGetByNamespace(req->key_prefix, namespace_);
            break;
        }
        /// Never used in MetaStoreDispatcher, obsoleted
        case Coordination::KVOpNum::MULTIGET:
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Not support 'MULTIGET' request yet.");
        default:
            break;
    }

    return response;
}

void MetaStoreConnection::waitInit()
{
    int64_t wait_time = 0;
    while(wait_time < startup_timeout)
    {
        try
        {
            /// ensure the remote raft cluster is ready
            localRangeGetByNamespace("", ProtonConsts::UDF_METASTORE_NAMESPACE);
            return;
        }
        catch(...)
        {
            LOG_DEBUG(log, "waiting for raft cluster...");
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            wait_time += 200;
        }
    }

    throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT cluster");
}

std::pair<String, Int32> MetaStoreConnection::forwardRequest(
    const String & method,
    const String & query_id,
    const String & user,
    const String & password,
    const Poco::JSON::Object::Ptr & payload,
    const String & uri_parameter) const
{
    constexpr auto * METASTORE_URL = "http://{}:{}/proton/metastore{}";
    std::vector<Poco::URI> failed_server;

    String body;
    if (payload)
    {
        std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(req_body_stream, 0);
        body = req_body_stream.str();
    }

    for (auto it = active_servers.begin(); it != active_servers.end();)
    {
        Poco::URI uri{fmt::format({METASTORE_URL}, it->getHost(), it->getPort(), uri_parameter)};
        auto [response, http_status] = sendRequest(uri, method, query_id, user, password, body, {}, ConnectionTimeouts({2, 0}, {5, 0}, {10, 0}) , log);

        if (http_status != Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR)
        {
            return {response, http_status};
        }
        it++;
        /*failed_server.emplace_back(*it);
        it = active_servers.erase(it);*/
    }

    /// all active servers fail, try inactive server
    //    for (auto it = active_servers.begin(); it != active_servers.end();)
    //    {
    //        auto [response, http_status] = sendRequest(
    //            *it,
    //            Poco::Net::HTTPRequest::HTTP_POST,
    //            ctx->getCurrentQueryId(),
    //            ctx->getUserName(),
    //            ctx->getPasswordByUserName(ctx->getUserName()),
    //            body,
    //            {},
    //            log);
    //
    //        if (http_status != Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR)
    //        {
    //            inactive_servers.erase(it);
    //            inactive_servers.insert(inactive_servers.end(), failed_server.begin(), failed_server.end());
    //            return {response, http_status};
    //        }
    //    }

    /// Compose the error response from a response of a forward request
    auto resp_builder = [&](const String & response, int error_code) {
        std::stringstream error_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Object error_resp;

        error_resp.set("error_msg", response);
        error_resp.set("code", error_code);
        error_resp.set("request_id", query_id);
        error_resp.stringify(error_str_stream, 0);
        return error_str_stream.str();
    };

    return {
        resp_builder("All metastore servers failed", Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR),
        Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
}
}
