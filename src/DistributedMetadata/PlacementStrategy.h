#pragma once

#include "Node.h"

#include <common/ClockUtils.h>

#include <boost/noncopyable.hpp>

#include <unordered_map>
#include <vector>

namespace DB
{
using DiskSpace = std::unordered_map<String, UInt64>; /// (policy name, free disk space)

struct NodeMetrics
{
    Node node;

    /// `(policy name, free disk space)`
    DiskSpace disk_space;

    Int64 last_heart_beat = -1;
    Int64 last_update_time = -1;
    Int64 num_of_tables = -1;

    bool staled = false;

    NodeMetrics(const String & node_identity_, const std::unordered_map<String, String> & headers) : node(node_identity_, headers)
    {
        auto iter = headers.find("_broadcast_time");
        if (iter != headers.end())
        {
            last_heart_beat = std::stoll(iter->second);
        }

        iter = headers.find("_tables");
        if (iter != headers.end())
        {
            num_of_tables = std::stoll(iter->second);
        }

        last_update_time = MonotonicMilliseconds::now();
    }

    Poco::JSON::Object::Ptr json() const
    {
        auto json_obj = node.json();

        json_obj->set("num_of_tables", num_of_tables);
        json_obj->set("last_heart_beat", last_heart_beat);

        Poco::JSON::Object::Ptr disk_space_obj(new Poco::JSON::Object());
        for (const auto & policy_space : disk_space)
        {
            disk_space_obj->set(policy_space.first, policy_space.second);
        }
        json_obj->set("disk_space", disk_space_obj);

        return json_obj;
    }

    bool isValid() const { return last_heart_beat > 0 && num_of_tables >= 0 && node.isValid(); }
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
