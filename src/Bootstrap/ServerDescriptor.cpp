#include <Bootstrap/ServerDescriptor.h>

#include <IO/ReadHelpers.h>
#include <base/getFQDNOrHostName.h>
#include <base/getMemoryAmount.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/getNumberOfPhysicalCPUCores.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void ServerDescriptor::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    /// Identity
    node_uuid = DB::UUIDHelpers::generateV4();
    hostname = getFQDNOrHostName();
    cluster_id = config.getString("cluster_id", "");
    secret = config.getString("intercluster_secret", "");

    /// Ports from config - check both plain and secure variants
    /// TCP port
    tcp_port = cluster::TCPPort(config.getUInt("node.tcp.port", 8463), config.getBool("node.tcp.is_tls_port", false));

    /// HTTP port
    http_port = cluster::TCPPort(config.getUInt("node.http.port", 3218), config.getBool("node.http.is_tls_port", false));

    /// gRPC port
    grpc_port = cluster::TCPPort(config.getUInt("grpc.port", 8090), config.getBool("grpc.is_tls_port", false));

    /// Table TCP port
    table_tcp_port = cluster::TCPPort(config.getUInt("node.table_tcp.port", 7587), config.getBool("node.table_tcp.is_tls_port", false));

    /// Table HTTP port
    table_http_port = cluster::TCPPort(config.getUInt("node.table_http.port", 8123), config.getBool("node.table_http.is_tls_port", false));

    /// PostgreSQL port
    postgresql_port = cluster::TCPPort(config.getUInt("node.postgresql.port", 5432), config.getBool("node.postgresql.is_tls_port", false));

    /// Paths
    data_path = config.getString("path", "./");
    config_path = config.getString("config_path", "");

    /// System info
    total_cpus = getNumberOfPhysicalCPUCores();
    total_memory_mb = getMemoryAmount() / (1024 * 1024);

    /// Boot time
    boot_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

double ServerDescriptor::getAverageDiskUsage() const
{
    /// No lock needed - intentional to avoid slowing the system
    /// Minor race conditions are acceptable for monitoring metrics
    if (disk_utils.empty())
        return 0.0;

    double total = 0.0;
    size_t count = 0;
    for (const auto & [_, stats] : disk_utils)
    {
        total += stats.util;
        count++;
    }

    return count > 0 ? (total / count) * 100.0 : 0.0;
}

}
