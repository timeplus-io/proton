#pragma once

#include <Cluster/Common/NodeID.h>
#include <Interpreters/Context_fwd.h>

#include <set>

namespace DB
{
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/// Single-instance: Create cluster with default node_id=1
ClusterPtr getCluster(ContextPtr context, cluster::NodeID node_id = 1);

/// proton: starts - Single-instance: These multi-node functions are no longer needed
/// ClusterPtr getClusterParallelShard(const std::set<cluster::NodeID> & node_ids, ContextPtr context);
/// ClusterPtr getClusterSeparateShards(const std::set<cluster::NodeID> & node_ids, ContextPtr context);
/// proton: ends
}
