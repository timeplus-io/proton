#pragma once

#include <memory>
#include <string>
#include <vector>

namespace DWAL
{

struct Partition
{
    int32_t id; /// Parititon ID
    int32_t leader; /// Leader replica node
    std::vector<int32_t> replica_nodes; /// All replica nodes
    std::vector<int32_t> isrs; /// In-Sync-Replicas nodes
};

struct KafkaWALInfo
{
    std::string name;
    std::vector<Partition> partitions;
};

struct KafkaNode
{
    int32_t id; /// DWAL Node ID
    int32_t port; /// Node listening port
    std::string host; /// Node hostname
};

struct KafkaWALCluster
{
    std::string id; /// Cluster ID
    int32_t controller_id; /// Controller node
    std::vector<KafkaNode> nodes;
};

using KafkaWALClusterPtr = std::shared_ptr<KafkaWALCluster>;
}
