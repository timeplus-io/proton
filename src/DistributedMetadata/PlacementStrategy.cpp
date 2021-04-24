#include "PlacementStrategy.h"

#include <random>

namespace DB
{
std::vector<NodeMetricsPtr> DiskStrategy::qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request)
{
    if (request.requested_nodes <= 0)
    {
        return {};
    }

    std::vector<NodeMetricsPtr> qualified_nodes;
    qualified_nodes.reserve(nodes_metrics.size());

    for (const auto & [node, metrics] : nodes_metrics)
    {
        auto iter = metrics->disk_space.find(request.storage_policy);
        if (!metrics->staled && iter != metrics->disk_space.end() && iter->second > 0)
        {
            qualified_nodes.emplace_back(metrics);
        }
    }

    if (qualified_nodes.size() < request.requested_nodes)
    {
        return {};
    }

    /// Shuffle qualified_nodes
    std::shuffle(std::begin(qualified_nodes), std::end(qualified_nodes), std::default_random_engine{});

    std::sort(
        std::begin(qualified_nodes),
        std::end(qualified_nodes),
        [&](const auto & lhs, const auto & rhs) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            auto l_disk_size = lhs->disk_space[request.storage_policy];
            auto r_disk_size = rhs->disk_space[request.storage_policy];
            if (l_disk_size == r_disk_size)
            {
                return lhs->num_of_tables < rhs->num_of_tables;
            }
            return l_disk_size > r_disk_size;
        });

    return {qualified_nodes.begin(), qualified_nodes.begin() + request.requested_nodes};
}

}
