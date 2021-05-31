#pragma once

#include <memory>
#include <string>
#include <vector>

namespace DB
{
namespace DWAL
{

struct Partition
{
    int32_t id; /// Parititon ID
    int32_t leader; /// Leader replica node
    std::vector<int32_t> replica_nodes; /// All replica nodes
    std::vector<int32_t> isrs; /// In-Sync-Replicas nodes
};

struct WALInfo
{
    std::string name;
    std::vector<Partition> partitions;
};

struct Node
{
    int32_t id; /// DWAL Node ID
    int32_t port; /// Node listening port
    std::string host; /// Node hostname
};

struct Cluster
{
    std::string id; /// Cluster ID
    int32_t controller_id; /// Controller node
    std::vector<Node> nodes;
};

using ClusterPtr = std::shared_ptr<Cluster>;
}
}
