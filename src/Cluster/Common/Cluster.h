#pragma once

#include <Bootstrap/ServerDescriptor.h>
#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/PortAddress.h>

#include <Common/SharedMutex.h>


namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{
struct ServerDescriptor;
using ServerDescriptorPtr = std::shared_ptr<ServerDescriptor>;
}

namespace cluster
{
/// Simplified cluster for single-node deployment using ServerDescriptor
class Cluster
{
public:
    explicit Cluster(const std::string & cid_);

    const auto & id() const noexcept { return cid; }

    void setServerDescriptor(const DB::ServerDescriptorPtr & server_desc);
    DB::ServerDescriptorPtr getServerDescriptor() const;
    bool hasNode(NodeID node_id) const noexcept { return node_id == 1; } /// Always true for node_id=1

    /// Always returns [1]
    std::vector<NodeID> nodeIDs() const { return {1}; }

    /// std::optional<NodeID> outstandingDecommissioningNode() const noexcept;

    HostPortPtr hostPortPublic(NodeID node_id) const noexcept
    {
        if (node_id != 1)
            return nullptr;
        std::shared_lock rlock{mu};
        if (server_descriptor)
            return std::make_shared<HostPort>(server_descriptor->hostname, server_descriptor->tcp_port.port);
        return nullptr;
    }

    HostPortPtr hostHTTPPortPublic(NodeID node_id) const noexcept
    {
        if (node_id != 1)
            return nullptr;
        std::shared_lock rlock{mu};
        if (server_descriptor)
            return std::make_shared<HostPort>(server_descriptor->hostname, server_descriptor->http_port.port);
        return nullptr;
    }

    /// Internal meta port not needed
    HostPortPtr hostPortInternalMeta(NodeID /*node_id*/) const noexcept { return nullptr; }

    /// Internal data port not needed
    HostPortPtr hostPortInternalData(NodeID /*node_id*/) const noexcept { return nullptr; }

    bool empty() const noexcept
    {
        std::shared_lock rlock{mu};
        return server_descriptor == nullptr;
    }

    size_t size() const noexcept { return server_descriptor ? 1 : 0; }

    /// bootstrap node management removed
    /// CallResult<NodeUUID> addBootstrappingMetaNode(cluster::NodeDescriptorPtr node_descriptor);
    /// bool removeBootstrappingMetaNode(const cluster::NodeDescriptorPtr & node_descriptor);

    auto lastUpdatedTimestamp() const noexcept { return last_update_ts; }


    /// Check if the given host:port matches any node in the cluster
    bool isLocalNode(const std::string & host, uint16_t tcp_port, const std::string & fqdn_name) const noexcept;


private:
    const std::string cid;

    mutable DB::SharedMutex mu;

    DB::ServerDescriptorPtr server_descriptor;

    int64_t last_update_ts = 0;

    LoggerPtr logger;
};

using ClusterPtr = std::shared_ptr<Cluster>;
}
