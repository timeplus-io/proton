#include <Cluster/Common/Cluster.h>

#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <chrono>

namespace DB::ErrorCodes
{
const extern int NODE_ID_ALREADY_ASSIGNED;
const extern int CLUSTER_ALREADY_INITED;
}

namespace cluster
{

Cluster::Cluster(const std::string & cid_) : cid(cid_), logger(getLogger("cluster"))
{
}


void Cluster::setServerDescriptor(const DB::ServerDescriptorPtr & server_desc)
{
    std::unique_lock lock(mu);
    server_descriptor = server_desc;
    last_update_ts = std::chrono::system_clock::now().time_since_epoch().count();
}

DB::ServerDescriptorPtr Cluster::getServerDescriptor() const
{
    std::shared_lock lock(mu);
    return server_descriptor;
}


bool Cluster::isLocalNode(const std::string & host, uint16_t tcp_port, const std::string & fqdn_name) const noexcept
{
    std::shared_lock lock(mu);
    if (!server_descriptor)
        return false;

    if (server_descriptor->tcp_port.port != tcp_port)
        return false;

    /// Check various host representations
    if (host == server_descriptor->hostname || host == fqdn_name)
        return true;

    /// Check localhost variants
    if (host == "localhost" || host == "127.0.0.1" || host == "::1")
        return true;

    return false;
}
}
