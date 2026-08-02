#include <Bootstrap/Globals.h>
#include <Bootstrap/ServerDescriptor.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/defines.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
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

        /// Per block device IO rates, keyed by device name.
        /// Values are ordered: read_iops, write_iops, read_throughput, write_throughput.
        {"disk_io_stats",
         std::make_shared<DataTypeMap>(
             std::make_shared<DataTypeString>(),
             std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()))},
        {"disk_io_stats_updated", std::make_shared<DataTypeDateTime64>(3)},

        /// Status
        {"status", std::make_shared<DataTypeString>()},
        {"uptime_seconds", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemServer::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    /// Get server descriptor from Globals
    const auto & server = Globals::getServerDescriptor();

    /// Column order must match getNamesAndTypes() above. Use a running index rather than
    /// literal positions, so inserting a column does not require renumbering everything after it.
    size_t i = 0;

    /// Basic identification
    res_columns[i++]->insert(server.node_id);
    res_columns[i++]->insert(DB::toString(server.node_uuid));
    res_columns[i++]->insert(server.hostname);
    res_columns[i++]->insert(server.cluster_id);

    /// Server ports from ServerDescriptor
    res_columns[i++]->insert(server.tcp_port.port);
    res_columns[i++]->insert(server.http_port.port);
    res_columns[i++]->insert(server.grpc_port.port);
    res_columns[i++]->insert(server.table_tcp_port.port);
    res_columns[i++]->insert(server.table_http_port.port);
    res_columns[i++]->insert(server.postgresql_port.port);

    /// Metrics from ServerDescriptor
    res_columns[i++]->insert(server.memory_used_mb.load(std::memory_order_relaxed));
    res_columns[i++]->insert(server.os_memory_free_mb.load(std::memory_order_relaxed));
    res_columns[i++]->insert(server.cpu_usage.load(std::memory_order_relaxed));
    res_columns[i++]->insert(server.os_cpu_usage.load(std::memory_order_relaxed));

    /// Get average disk usage from ServerDescriptor method
    res_columns[i++]->insert(server.getAverageDiskUsage());

    /// Total materialized views count
    res_columns[i++]->insert(server.total_materialized_views.load(std::memory_order_relaxed));

    /// Total shards count
    res_columns[i++]->insert(server.total_shards.load(std::memory_order_relaxed));

    /// disk_io_stats map(string, array(float64)), values ordered:
    /// read_iops, write_iops, read_throughput, write_throughput
    {
        auto * column_map = typeid_cast<ColumnMap *>(res_columns[i].get());
        auto & offsets = column_map->getNestedColumn().getOffsets();
        auto & tuple_column = column_map->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_array_column = assert_cast<ColumnArray &>(tuple_column.getColumn(1));
        auto & value_offsets = value_array_column.getOffsets();
        auto & value_data_column = value_array_column.getData();

        size_t map_size = 0;
        for (const auto & [disk_name, stats] : server.disk_io_stats)
        {
            key_column.insertData(disk_name.data(), disk_name.size());
            value_data_column.insert(stats.read_iops);
            value_data_column.insert(stats.write_iops);
            value_data_column.insert(stats.read_throughput);
            value_data_column.insert(stats.write_throughput);

            const auto prev_value_offset = value_offsets.empty() ? 0 : value_offsets.back();
            value_offsets.push_back(prev_value_offset + 4);
            ++map_size;
        }

        const auto prev_offset = offsets.empty() ? 0 : offsets.back();
        offsets.push_back(prev_offset + map_size);
        ++i;
    }

    res_columns[i++]->insert(DateTime64(server.disk_io_stats_updated_ms.load(std::memory_order_relaxed)));

    /// Status
    res_columns[i++]->insert("active"); /// Always active if we're querying it

    /// Calculate uptime from boot timestamp
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t uptime_seconds = (now_ms - server.boot_timestamp) / 1000;
    res_columns[i++]->insert(uptime_seconds);

    /// Catches a fill that was forgotten or added without a matching column above.
    chassert(i == res_columns.size());
}
}
