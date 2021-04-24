#pragma once

#include <common/types.h>

#include <boost/noncopyable.hpp>

#include <unordered_map>
#include <vector>

namespace DB
{
using DiskSpace = std::unordered_map<String, UInt64>; /// (policy name, free disk space)
struct NodeMetrics
{
    /// `host` is network reachable like hostname, FQDN or IP of the node
    String host;
    /// `channel` is the identifier of the host exposed to client for polling status
    String channel;
    /// `node_identity` can be unique uuid
    String node_identity;
    /// `(policy name, free disk space)`
    DiskSpace disk_space;

    String http_port;
    String tcp_port;

    Int64 last_update_time = -1;
    Int64 num_of_tables = -1;

    bool staled = false;

    explicit NodeMetrics(const String & host_, const String & channel_) : host(host_), channel(channel_) { }
};
using NodeMetricsPtr = std::shared_ptr<NodeMetrics>;
using NodeMetricsContainer = std::unordered_map<String, NodeMetricsPtr>;

class PlacementStrategy : private boost::noncopyable
{
public:
    struct PlacementRequest
    {
        size_t requested_nodes;
        String storage_policy;
    };

    PlacementStrategy() = default;
    virtual ~PlacementStrategy() = default;
    virtual std::vector<NodeMetricsPtr> qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request) = 0;
};

class DiskStrategy final : public PlacementStrategy
{
public:
    std::vector<NodeMetricsPtr> qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request) override;
};

using PlacementStrategyPtr = std::shared_ptr<PlacementStrategy>;

}
