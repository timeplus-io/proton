#include "PlacementService.h"

#include "CatalogService.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
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
const String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";

const size_t RESCHEDULE_INTERVAL_MS = 5000;
const Int64 STALENESS_THRESHOLD_MS = 10000;
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
    const auto & config = global_context->getConfigRef();
    reschedule_interval = config.getUInt("cluster_settings.schedule_intervals.node_metrics", RESCHEDULE_INTERVAL_MS);
}

MetadataService::ConfigSettings PlacementService::configSettings() const
{
    return {
        .key_prefix = PLACEMENT_KEY_PREFIX,
        .default_name = PLACEMENT_DEFAULT_TOPIC,
        .default_data_retention = 2,
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
                node_metrics->node.host,
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

    /// FIXME, return NodeMetrics directly
    for (const auto & t : tables)
    {
        /// FIXME, https
        auto iter = nodes_metrics.find(t->node_identity);
        if (iter != nodes_metrics.end())
        {
            hosts.push_back(iter->second->node.host + ":" + std::to_string(iter->second->node.http_port));
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

void PlacementService::preShutdown()
{
    if (broadcast_task)
        (*broadcast_task)->deactivate();
}

void PlacementService::processRecords(const DWAL::RecordPtrs & records)
{
    /// Node metrics schema: node, disk_free
    for (const auto & record : records)
    {
        assert(record->op_code == DWAL::OpCode::ADD_DATA_BLOCK);
        if (!record->hasIdempotentKey())
        {
            LOG_ERROR(log, "Invalid metric record, missing idempotent key");
            continue;
        }

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

void PlacementService::mergeMetrics(const String & node_identity, const DWAL::RecordPtr & record)
{
    NodeMetricsPtr node_metrics = std::make_shared<NodeMetrics>(node_identity, record->headers);
    if (!node_metrics->isValid() || node_metrics->node.channel.empty())
    {
        LOG_ERROR(log, "Invalid metric record: {}", node_metrics->node.string());
        return;
    }

    DiskSpace disk_space;
    disk_space.reserve(record->block.rows());

    for (size_t row = 0; row < record->block.rows(); ++row)
    {
        const auto & policy_name = record->block.getByName("policy_name").column->getDataAt(row);
        const auto space = record->block.getByName("disk_space").column->get64(row);
        LOG_TRACE(log, "Receive disk space data from {}. Storage policy={}, Disk size={}GB", node_identity, policy_name, space);
        disk_space.emplace(policy_name, space);
    }

    node_metrics->disk_space.swap(disk_space);

    bool staled = false;
    {
        std::unique_lock guard(rwlock);

        auto iter = nodes_metrics.find(node_identity);
        if (iter != nodes_metrics.end())
        {
            /// Existing node metrics.
            staled = iter->second->staled;
            iter->second = node_metrics;
        }
        else
        {
            /// New node metrics.
            nodes_metrics.emplace(node_identity, node_metrics);
        }
    }

    if (staled)
    {
        LOG_INFO(log, "Node identity={} host={} recovered from staleness", node_metrics->node.identity, node_metrics->node.host);
    }
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
    (*broadcast_task)->scheduleAfter(reschedule_interval);
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

    DWAL::Record record{DWAL::OpCode::ADD_DATA_BLOCK, std::move(disk_block)};
    record.partition_key = 0;
    setupRecordHeaders(record, "1");

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
    if (result.err != ErrorCodes::OK)
    {
        /// FIXME: Error handling
        LOG_ERROR(log, "Failed to append node metrics error={}", result.err);
        return;
    }

    LOG_DEBUG(log, "Appended node metrics");
}

}
