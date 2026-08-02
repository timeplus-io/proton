#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/PortAddress.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Holds all server configuration and runtime metrics for  deployment
/// Loaded once during bootstrap, no config reload complexity
struct ServerDescriptor
{
    /// Identity
    cluster::NodeID node_id = 1; // Always 1 for
    DB::UUID node_uuid;
    String hostname;
    String cluster_id = "";
    String secret;

    /// Ports (loaded from config during bootstrap) - using existing cluster::TCPPort
    cluster::TCPPort tcp_port{8463, false};
    cluster::TCPPort http_port{3218, false};
    cluster::TCPPort grpc_port{8090, false};
    cluster::TCPPort table_tcp_port{7587, false};
    cluster::TCPPort table_http_port{8123, false};
    cluster::TCPPort postgresql_port{5432, false};

    /// Paths
    String data_path;
    String config_path;

    /// System info (static)
    UInt32 total_cpus = 0;
    UInt64 total_memory_mb = 0;

    /// Runtime metrics (updated by AsynchronousMetrics)
    std::atomic<UInt32> memory_used_mb{0};
    std::atomic<UInt32> os_memory_free_mb{0};
    std::atomic<double> cpu_usage{0.0};
    std::atomic<double> os_cpu_usage{0.0};
    std::atomic<UInt32> total_materialized_views{0}; /// Count of running MVs (updated by StreamStateLog)
    std::atomic<UInt32> total_shards{0}; /// Total physical shards across all streams

    /// Disk metrics (updated by AsynchronousMetrics)
    struct DiskStats
    {
        UInt64 total_mb = 0;
        UInt64 free_mb = 0;
        double util = 0;
    };

    /// Disk volume name to DiskStats map
    /// We don't hold a lock around disk_utils map since we assume during the startup of the program
    /// AsynchronousMetrics will populate this map initially and after that the disk volumes stays
    /// intact across the whole life of the program process. So future update from different thread,
    /// or read from different thread is totally fine since the keys are not changing.
    /// Minor race conditions are intentional to avoid collection slowing the system.
    std::unordered_map<String, DiskStats> disk_utils;

    /// Per block device IO rates (updated by AsynchronousMetrics)
    struct DiskIOStats
    {
        double read_iops = 0; /// Read operations per second
        double write_iops = 0; /// Write operations per second
        double read_throughput = 0; /// Read bytes per second
        double write_throughput = 0; /// Write bytes per second
    };

    /// Block device name to DiskIOStats map. The same no-lock reasoning as `disk_utils` applies:
    /// the device set is discovered on the first collection and does not change afterwards.
    std::unordered_map<String, DiskIOStats> disk_io_stats;

    /// When `disk_io_stats` was last refreshed, in milliseconds since epoch. Zero until the first
    /// pair of samples is available, since a rate needs two.
    std::atomic<UInt64> disk_io_stats_updated_ms{0};

    /// Status
    UInt64 boot_timestamp = 0;

    /// Load from config during bootstrap
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);

    /// Get average disk usage
    double getAverageDiskUsage() const;
};

using ServerDescriptorPtr = std::shared_ptr<ServerDescriptor>;

}
