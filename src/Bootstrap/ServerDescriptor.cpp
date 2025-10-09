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
    if (config.has("tcp_secure.port"))
        tcp_port = cluster::TCPPort(config.getUInt("tcp_secure.port"), true);
    else
        tcp_port = cluster::TCPPort(config.getUInt("tcp.port", 8463), false);

    /// HTTP port
    if (config.has("https.port"))
        http_port = cluster::TCPPort(config.getUInt("https.port"), true);
    else
        http_port = cluster::TCPPort(config.getUInt("http.port", 3218), false);

    /// gRPC port
    if (config.has("grpc_secure.port"))
        grpc_port = cluster::TCPPort(config.getUInt("grpc_secure.port"), true);
    else
        grpc_port = cluster::TCPPort(config.getUInt("grpc.port", 8090), false);

    /// Table TCP port
    if (config.has("table_tcp_secure.port"))
        table_tcp_port = cluster::TCPPort(config.getUInt("table_tcp_secure.port"), true);
    else
        table_tcp_port = cluster::TCPPort(config.getUInt("table_tcp.port", 7587), false);

    /// Table HTTP port
    if (config.has("table_https.port"))
        table_http_port = cluster::TCPPort(config.getUInt("table_https.port"), true);
    else
        table_http_port = cluster::TCPPort(config.getUInt("table_http.port", 8123), false);

    /// PostgreSQL port
    if (config.has("postgresql_secure.port"))
        postgresql_port = cluster::TCPPort(config.getUInt("postgresql_secure.port"), true);
    else
        postgresql_port = cluster::TCPPort(config.getUInt("postgresql.port", 5432), false);

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
