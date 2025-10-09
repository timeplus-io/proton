#include <Bootstrap/Globals.h>
#include <Bootstrap/ServerDescriptor.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemServer.h>

namespace DB
{

NamesAndTypesList StorageSystemServer::getNamesAndTypes()
{
    return {
        /// Basic identification
        {"node_id", std::make_shared<DataTypeUInt32>()},
        {"node_uuid", std::make_shared<DataTypeString>()},
        {"hostname", std::make_shared<DataTypeString>()},
        {"cluster_id", std::make_shared<DataTypeString>()},

        /// Server ports (important for verification)
        {"tcp_port", std::make_shared<DataTypeUInt16>()},
        {"http_port", std::make_shared<DataTypeUInt16>()},
        {"grpc_port", std::make_shared<DataTypeUInt16>()},
        {"table_tcp_port", std::make_shared<DataTypeUInt16>()},
        {"table_http_port", std::make_shared<DataTypeUInt16>()},
        {"postgresql_port", std::make_shared<DataTypeUInt16>()},

        /// Metrics
        {"memory_used_mb", std::make_shared<DataTypeUInt32>()},
        {"os_memory_free_mb", std::make_shared<DataTypeUInt32>()},
        {"cpu_usage", std::make_shared<DataTypeFloat64>()},
        {"os_cpu_usage", std::make_shared<DataTypeFloat64>()},
        {"disk_usage_percent", std::make_shared<DataTypeFloat64>()},
        {"total_materialized_views", std::make_shared<DataTypeUInt32>()},
        {"total_shards", std::make_shared<DataTypeUInt32>()},

        /// Status
        {"status", std::make_shared<DataTypeString>()},
        {"uptime_seconds", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemServer::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// Get server descriptor from Globals
    const auto & server = Globals::getServerDescriptor();

    /// Basic identification
    res_columns[0]->insert(server.node_id);
    res_columns[1]->insert(DB::toString(server.node_uuid));
    res_columns[2]->insert(server.hostname);
    res_columns[3]->insert(server.cluster_id);

    /// Server ports from ServerDescriptor
    res_columns[4]->insert(server.tcp_port.port);
    res_columns[5]->insert(server.http_port.port);
    res_columns[6]->insert(server.grpc_port.port);
    res_columns[7]->insert(server.table_tcp_port.port);
    res_columns[8]->insert(server.table_http_port.port);
    res_columns[9]->insert(server.postgresql_port.port);

    /// Metrics from ServerDescriptor
    res_columns[10]->insert(server.memory_used_mb.load(std::memory_order_relaxed));
    res_columns[11]->insert(server.os_memory_free_mb.load(std::memory_order_relaxed));
    res_columns[12]->insert(server.cpu_usage.load(std::memory_order_relaxed));
    res_columns[13]->insert(server.os_cpu_usage.load(std::memory_order_relaxed));

    /// Get average disk usage from ServerDescriptor method
    res_columns[14]->insert(server.getAverageDiskUsage());

    /// Total materialized views count
    res_columns[15]->insert(server.total_materialized_views.load(std::memory_order_relaxed));

    /// Total shards count
    res_columns[16]->insert(server.total_shards.load(std::memory_order_relaxed));

    /// Status
    res_columns[17]->insert("active"); /// Always active if we're querying it

    /// Calculate uptime from boot timestamp
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t uptime_seconds = (now_ms - server.boot_timestamp) / 1000;
    res_columns[18]->insert(uptime_seconds);
}
}
