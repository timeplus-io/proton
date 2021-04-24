#include "PlacementService.h"

#include "CatalogService.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <common/ClockUtils.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

#include <city.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
/// Globals
const String PLACEMENT_KEY_PREFIX = "cluster_settings.system_node_metrics.";
const String PLACEMENT_NAME_KEY = PLACEMENT_KEY_PREFIX + "name";
const String PLACEMENT_REPLICATION_FACTOR_KEY = PLACEMENT_KEY_PREFIX + "replication_factor";
const String PLACEMENT_DATA_RETENTION_KEY = PLACEMENT_KEY_PREFIX + "data_retention";
const String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";

const String THIS_HOST = getFQDNOrHostName();
}

PlacementService & PlacementService::instance(const ContextPtr & context)
{
    static PlacementService placement{context};
    return placement;
}

PlacementService::PlacementService(const ContextPtr & global_context_) : PlacementService(global_context_, std::make_shared<DiskStrategy>())
{
}

PlacementService::PlacementService(const ContextPtr & global_context_, PlacementStrategyPtr strategy_)
    : MetadataService(global_context_, "PlacementService"), catalog(CatalogService::instance(global_context_)), strategy(strategy_)
{
}

MetadataService::ConfigSettings PlacementService::configSettings() const
{
    return {
        .name_key = PLACEMENT_NAME_KEY,
        .default_name = PLACEMENT_DEFAULT_TOPIC,
        .data_retention_key = PLACEMENT_DATA_RETENTION_KEY,
        .default_data_retention = 2,
        .replication_factor_key = PLACEMENT_REPLICATION_FACTOR_KEY,
        .request_required_acks = 1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
        .initial_default_offset = -2,
    };
}

std::vector<NodeMetricsPtr> PlacementService::place(
    Int32 shards, Int32 replication_factor, const String & storage_policy /*= "default"*/, const String & /* colocated_table */) const
{
    size_t total_replicas = static_cast<size_t>(shards * replication_factor);
    PlacementStrategy::PlacementRequest request{total_replicas, storage_policy};

    std::shared_lock guard{rwlock};

    for (const auto & [node_identity, node_metrics] : nodes_metrics)
    {
        auto staleness = MonotonicMilliseconds::now() - node_metrics->last_update_time;
        if (staleness > STALENESS_THRESHOLD_MS)
        {
            node_metrics->staled = true;
            LOG_WARNING(
                log,
                "Node identity={} host={} is staled. Didn't hear from it since last update={} for {}ms",
                node_identity,
                node_metrics->host,
                node_metrics->last_update_time,
                staleness);
        }
    }

    return strategy->qualifiedNodes(nodes_metrics, request);
}

std::vector<String> PlacementService::placed(const String & database, const String & table) const
{
    auto tables{catalog.findTableByName(database, table)};

    std::vector<String> hosts;
    hosts.reserve(tables.size());

    std::shared_lock guard(rwlock);

    for (const auto & t : tables)
    {
        if (nodes_metrics.contains(t->node_identity))
        {
            hosts.push_back(t->host + ":" + nodes_metrics.at(t->node_identity)->http_port);
        }
        else
        {
            hosts.push_back(t->host);
        }
    }
    return hosts;
}

std::vector<NodeMetricsPtr> PlacementService::nodes() const
{
    std::vector<NodeMetricsPtr> nodes;

    std::shared_lock guard{rwlock};
    nodes.reserve(nodes_metrics.size());

    for (const auto & [node, metrics] : nodes_metrics)
    {
        nodes.emplace_back(metrics);
    }

    return nodes;
}

String PlacementService::getNodeIdentityByChannel(const String & channel) const
{
    std::shared_lock guard(rwlock);

    for (const auto & [node_identity, node_metrics] : nodes_metrics)
    {
        if(!node_metrics->staled && node_metrics->channel == channel)
        {
            return node_metrics->host;
        }
    }

    return "";
}

void PlacementService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    /// Node metrics schema: node, disk_free
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);
        if (record->headers["_version"] == "1")
        {
            mergeMetrics(record->idempotentKey(), record);
        }
        else
        {
            LOG_ERROR(log, "Cannot handle version={}", record->headers["_version"]);
        }
    }
}

void PlacementService::mergeMetrics(const String & key, const IDistributedWriteAheadLog::RecordPtr & record)
{
    for (const auto & item : {"_host", "_channel", "_http_port", "_tcp_port", "_tables"})
    {
        if (!record->headers.contains(item))
        {
            LOG_ERROR(log, "Invalid metric record. '{}', '{}' not found", key, item);
            return;
        }
    }

    const String & host = record->headers["_host"];
    const String & channel_id = record->headers["_channel"];
    const String & http_port = record->headers["_http_port"];
    const String & tcp_port = record->headers["_tcp_port"];
    const Int64 table_counts = std::stoll(record->headers["_tables"]);
    const auto broadcast_time = std::stoll(record->headers["_broadcast_time"]);

    DiskSpace disk_space;
    for (size_t row = 0; row < record->block.rows(); ++row)
    {
        const auto & policy_name = record->block.getByName("policy_name").column->getDataAt(row);
        const auto space = record->block.getByName("disk_space").column->get64(row);
        LOG_TRACE(log, "Receive disk space data from {}. Storage policy={}, Disk size={}GB", key, policy_name, space);
        disk_space.emplace(policy_name, space);
    }

    std::unique_lock guard(rwlock);

    NodeMetricsPtr node_metrics;
    auto iter = nodes_metrics.find(key);
    if (iter == nodes_metrics.end())
    {
        /// New node metrics.
        node_metrics = std::make_shared<NodeMetrics>(host, channel_id);
        nodes_metrics.emplace(key, node_metrics);
    }
    else
    {
        /// Existing node metrics.
        node_metrics = iter->second;
        auto utc_now = UTCMilliseconds::now();
        if (utc_now < broadcast_time)
        {
            LOG_WARNING(
                log,
                "The broadcast time from node identity={} host={} is ahead of local time. Clocks between the machines are out of sync.",
                key,
                host);
        }
        else if (auto latency = utc_now - broadcast_time; latency > LATENCY_THRESHOLD_MS)
        {
            LOG_TRACE(
                log,
                "It took {}ms to broadcast node metrics from node identity={} host={}. Probably there is some perf issue or the clocks "
                "between the machines are out of sync too much.",
                latency,
                key,
                host);
        }

        if (node_metrics->staled)
        {
            node_metrics->staled = false;
            LOG_INFO(log, "Node identity={} host={} recovered from staleness", key, host);
        }
    }
    node_metrics->channel = channel_id;
    node_metrics->node_identity = key;
    node_metrics->http_port = http_port;
    node_metrics->tcp_port = tcp_port;
    node_metrics->disk_space.swap(disk_space);
    node_metrics->num_of_tables = table_counts;
    node_metrics->last_update_time = MonotonicMilliseconds::now();
}

void PlacementService::scheduleBroadcast()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    /// Schedule the broadcast task
    auto task_holder = global_context->getSchedulePool().createTask("PlacementBroadcast", [this]() { this->broadcast(); });
    broadcast_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*broadcast_task)->activate();
    (*broadcast_task)->schedule();
}

void PlacementService::broadcast()
{
    try
    {
        doBroadcast();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to broadcast node metrics, error={}", getCurrentExceptionMessage(true, true));
    }
    (*broadcast_task)->scheduleAfter(RESCHEDULE_INTERNAL_MS);
}

void PlacementService::doBroadcast()
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get(getTypeName(TypeIndex::String));
    auto uint64_type = data_type_factory.get(getTypeName(TypeIndex::UInt64));

    auto policy_name_col = string_type->createColumn();
    auto disk_space_col = uint64_type->createColumn();
    auto * disk_space_col_inner = typeid_cast<ColumnUInt64 *>(disk_space_col.get());

    for (const auto & [policy_name, policy_ptr] : global_context->getPoliciesMap())
    {
        const auto disk_space = policy_ptr->getMaxUnreservedFreeSpace() / (1024 * 1024 * 1024);
        policy_name_col->insertData(policy_name.data(), policy_name.size());
        disk_space_col_inner->insertValue(disk_space);
        LOG_TRACE(log, "Append disk metrics {}, {}, {} GB.", global_context->getNodeIdentity(), policy_name, disk_space);
    }

    Block disk_block;
    ColumnWithTypeAndName policy_name_col_with_type(std::move(policy_name_col), string_type, "policy_name");
    disk_block.insert(policy_name_col_with_type);
    ColumnWithTypeAndName disk_space_col_with_type{std::move(disk_space_col), uint64_type, "disk_space"};
    disk_block.insert(disk_space_col_with_type);

    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(disk_block)};
    record.partition_key = 0;
    record.setIdempotentKey(global_context->getNodeIdentity());
    record.headers["_host"] = THIS_HOST;
    record.headers["_channel"] = global_context->getChannel();
    record.headers["_http_port"] = global_context->getConfigRef().getString("http_port", "8123");
    record.headers["_tcp_port"] = global_context->getConfigRef().getString("tcp_port", "9000");
    record.headers["_version"] = "1";

    const String table_count_query = "SELECT count(*) as table_counts FROM system.tables WHERE database != 'system'";

    /// FIXME, QueryScope
    auto context = Context::createCopy(global_context);
    context->makeQueryContext();
    executeSelectQuery(table_count_query, context, [&record](Block && block) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        const auto & table_counts_col = block.findByName("table_counts")->column;
        record.headers["_tables"] = std::to_string(table_counts_col->getUInt(0));
    });

    record.headers["_broadcast_time"] = std::to_string(UTCMilliseconds::now());

    const auto & result = dwal->append(record, dwal_append_ctx);
    if (result.err == ErrorCodes::OK)
    {
        LOG_DEBUG(log, "Appended {} disk space records in one node metrics block", record.block.rows());
    }
    else
    {
        LOG_ERROR(log, "Failed to append node metrics block, error={}", result.err);
    }
}

}
