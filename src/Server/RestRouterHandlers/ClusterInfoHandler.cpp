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

std::pair<String, Int32> ClusterInfoHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    auto placement_nodes = CatalogService::instance(query_context).nodes("placement");
    if (placement_nodes.empty())
    {
        return {jsonErrorResponse("Internal server error", ErrorCodes::RESOURCE_NOT_INITED), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }

    const auto & identity = query_context->getNodeIdentity();
    for (const auto & node : placement_nodes)
    {
        if (node->identity == identity)
        {
            /// If the current node is one of the placement nodes
            /// Use the current node to serve the request directly
            return {getClusterInfoLocally(), HTTPResponse::HTTP_OK};
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
    auto [response, http_status] = sendRequest(
        uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        query_context->getCurrentQueryId(),
        query_context->getUserName(),
        query_context->getPasswordByUserName(query_context->getUserName()),
        "",
        log);

    if (http_status == HTTPResponse::HTTP_OK)
    {
        return {response, http_status};
    }

    return {jsonErrorResponseFrom(response), http_status};
}

String ClusterInfoHandler::getClusterInfoLocally() const
{
    auto nodes{PlacementService::instance(query_context).nodes()};

    Poco::JSON::Array json_nodes;
    for (const auto & node : nodes)
    {
        json_nodes.add(node->json());
    }

    Poco::JSON::Object resp;
    resp.set("nodes", json_nodes);
    resp.set("request_id", query_context->getCurrentQueryId());

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}
