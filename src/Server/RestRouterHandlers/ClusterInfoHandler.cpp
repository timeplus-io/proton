#include "ClusterInfoHandler.h"

#include <DistributedMetadata/CatalogService.h>
#include <DistributedMetadata/PlacementService.h>
#include <DistributedMetadata/sendRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_INITED;
}

namespace
{
    const String CLUSTER_INFO_URL = "http://{}:{}/dae/v1/clusterinfo";
}

String ClusterInfoHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    auto placement_nodes = CatalogService::instance(query_context).nodes("placement");
    if (placement_nodes.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
        return jsonErrorResponse("Internal server error", ErrorCodes::RESOURCE_NOT_INITED);
    }

    const auto & identity = query_context->getNodeIdentity();
    for (const auto & node : placement_nodes)
    {
        if (node->identity == identity)
        {
            /// If the current node is one of the placement nodes
            /// Use the current node to serve the request directly
            return buildResponse();
        }
    }

    /// Pick one placement node
    size_t pos = 0;
    if (placement_nodes.size() > 1)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> distrib(0, placement_nodes.size() - 1);
        pos = distrib(gen);
    }

    /// FIXME, https
    Poco::URI uri{fmt::format(CLUSTER_INFO_URL, placement_nodes[pos]->host, placement_nodes[pos]->http_port)};
    auto [response, http_code] = sendRequest(
        uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        query_context->getCurrentQueryId(),
        query_context->getUserName(),
        query_context->getPasswordByUserName(query_context->getUserName()),
        "",
        log);

    if (http_code == Poco::Net::HTTPResponse::HTTP_OK)
    {
        return response;
    }

    http_status = http_code;
    return buildErrorResponse(response);
}

String ClusterInfoHandler::buildResponse() const
{
    auto nodes{PlacementService::instance(query_context).nodes()};

    Poco::JSON::Array json_nodes;
    for (const auto & node : nodes)
    {
        json_nodes.add(node->json());
    }

    Poco::JSON::Object resp;
    resp.set("nodes", json_nodes);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String ClusterInfoHandler::buildErrorResponse(const String & response) const
{
    if (response.find("request_id") != String::npos && response.find("error_msg") != String::npos)
    {
        /// It is already a well-formed response from remote
        return response;
    }
    return jsonErrorResponse("Internal server error", ErrorCodes::RESOURCE_NOT_INITED);
}
}
