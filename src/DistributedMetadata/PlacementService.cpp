#include "PlacementService.h"

#include <NativeLog/Server/NativeLog.h>
#include <Storages/Streaming/StorageStream.h>
#include <Storages/Streaming/StreamShard.h>
#include "CatalogService.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <base/logger_useful.h>
#include <base/map.h>

#include <city.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNSUPPORTED;
    extern const int RESOURCE_NOT_FOUND;
    extern const int DISK_SIZE_EXCEED;
}

namespace
{
/// Globals
const String PLACEMENT_KEY_PREFIX = "cluster_settings.system_node_metrics.";
const String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";

const size_t RESCHEDULE_INTERVAL_MS = 5000;
const Int64 STALENESS_THRESHOLD_MS = 10000;
const UInt64 STORAGE_UPDATE_INTERVAL_S = 60;

String getStreamName(const String & database, const String & stream)
{
    return fmt::format("{}.{}", backQuoteIfNeed(database), backQuoteIfNeed(stream));
}
}

Poco::Dynamic::Var StorageInfoForStream::toJSON(bool is_simple) const
{
    Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);

    if (is_simple)
    {
        obj.set("bytes_on_disk", streaming_data_bytes + historical_data_bytes);
    }
    else
    {
        obj.set("streaming_data_bytes", streaming_data_bytes);
        Poco::JSON::Array streaming_paths(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & path : streaming_data_paths)
            streaming_paths.add(path);
        obj.set("streaming_data_paths", streaming_paths);

        obj.set("historical_data_bytes", historical_data_bytes);
        Poco::JSON::Array historical_paths(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & path : historical_data_paths)
            historical_paths.add(path);
        obj.set("historical_data_paths", historical_paths);
    }
    return obj;
}

Poco::Dynamic::Var StreamStorageInfo::toJSON(bool is_simple) const
{
    Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);
    obj.set("total_bytes_on_disk", total_bytes_on_disk);

    if (need_sort_by_bytes.has_value())
    {
        Poco::JSON::Object streams_info(Poco::JSON_PRESERVE_KEY_ORDER);
        auto streams_vec = collections::map<std::vector<StorageInfoForStream>>(streams, [](const auto & elem) { return elem.second; });

        /// Sorting
        if (*need_sort_by_bytes)
            std::sort(streams_vec.begin(), streams_vec.end(), [](const auto & s1, const auto & s2) {
                return (s1.streaming_data_bytes + s1.historical_data_bytes) > (s2.streaming_data_bytes + s2.historical_data_bytes);
            });
        else
            std::sort(streams_vec.begin(), streams_vec.end(), [](const auto & s1, const auto & s2) {
                return (s1.streaming_data_bytes + s1.historical_data_bytes) < (s2.streaming_data_bytes + s2.historical_data_bytes);
            });

        for (const auto & stream : streams_vec)
            streams_info.set(getStreamName(stream.id.database_name, stream.id.table_name), stream.toJSON(is_simple));
        obj.set("streams", streams_info);
    }
    else
    {
        Poco::JSON::Object streams_info;
        for (const auto & [_, stream] : streams)
            streams_info.set(getStreamName(stream.id.database_name, stream.id.table_name), stream.toJSON(is_simple));
        obj.set("streams", streams_info);
    }
    return obj;
}

PlacementService & PlacementService::instance(const ContextMutablePtr & context)
{
    static PlacementService placement{context};
    return placement;
}

PlacementService::PlacementService(const ContextMutablePtr & global_context_)
    : PlacementService(global_context_, std::make_shared<DiskStrategy>())
{
}

PlacementService::PlacementService(const ContextMutablePtr & global_context_, PlacementStrategyPtr strategy_)
    : MetadataService(global_context_, "PlacementService"), catalog(CatalogService::instance(global_context_)), strategy(strategy_)
{
    const auto & config = global_context->getConfigRef();
    reschedule_interval = config.getUInt(PLACEMENT_KEY_PREFIX + "schedule_intervals", RESCHEDULE_INTERVAL_MS);
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
    size_t shards, size_t replication_factor, const String & storage_policy /*= "default"*/, const String & /* colocated_table */) const
{
    PlacementStrategy::PlacementRequest request{replication_factor, shards * replication_factor, storage_policy};

    std::vector<NodeMetricsPtr> results;
    std::vector<std::pair<String, NodeMetricsPtr>> stale_nodes;
    {
        std::shared_lock guard{rwlock};

        for (const auto & [node_identity, node_metrics] : nodes_metrics)
        {
            auto staleness = MonotonicMilliseconds::now() - node_metrics->last_update_time;
            if (staleness > STALENESS_THRESHOLD_MS)
            {
                node_metrics->staled = true;
                stale_nodes.emplace_back(node_identity, node_metrics);
            }
        }
        results = strategy->qualifiedNodes(nodes_metrics, request);
    }

    for (const auto & stale_node : stale_nodes)
    {
        auto staleness = MonotonicMilliseconds::now() - stale_node.second->last_update_time;
        LOG_WARNING(
            log,
            "Node identity={} host={} is staled. Didn't hear from it since last update={} for {}ms",
            stale_node.first,
            stale_node.second->node.host,
            stale_node.second->last_update_time,
            staleness);
    }

    return results;
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

StreamStorageInfoPtr PlacementService::loadStorageInfo(const String & ns, const String & stream)
{
    if (storage_info_ptr && last_update_s > 0 && UTCSeconds::now() - last_update_s < STORAGE_UPDATE_INTERVAL_S)
        return storage_info_ptr;

    StreamStorageInfoPtr storage_info = std::make_shared<StreamStorageInfo>();
    /// Load from historical storage
    loadLocalStoragesInfo(storage_info, ns, stream);

    /// Load from streaming store
    nlog::NativeLog & native_log = nlog::NativeLog::instance(nullptr);
    if (native_log.enabled())
    {
        //        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "The native log is not enabled, so the interface does not work.");
        auto list_resp{native_log.listStreams(ns, nlog::ListStreamsRequest{stream})};
        if (list_resp.hasError())
            throw DB::Exception(list_resp.error_code, "Failed to list streams in streaming store: {}", list_resp.error_message);

        /// streams + 100 (reserved for other engines, such as system tables)
        storage_info->streams.reserve(list_resp.streams.size() + 100);
        for (const auto & stream_desc : list_resp.streams)
        {
            StorageID storage_id{stream_desc.ns, stream_desc.stream};
            auto & stream_info = storage_info->streams[storage_id.getFullNameNotQuoted()];
            stream_info.id = storage_id;

            auto streaming_data_info_opt = native_log.getLocalStreamInfo(stream_desc);
            if (!streaming_data_info_opt.has_value())
                throw DB::Exception(
                    DB::ErrorCodes::RESOURCE_NOT_FOUND,
                    "Failed to get info of the stream '{} in streaming store",
                    getStreamName(stream_desc.ns, stream_desc.stream));

            stream_info.streaming_data_bytes = streaming_data_info_opt->first;
            stream_info.streaming_data_paths = std::move(streaming_data_info_opt->second);
            storage_info->total_bytes_on_disk += stream_info.streaming_data_bytes;
        }
    }

    storage_info_ptr = std::move(storage_info);
    last_update_s = UTCSeconds::now();

    return storage_info_ptr;
}

void PlacementService::checkStorageQuotaOrThrow()
{
    const auto & disk_info = loadStorageInfo("", "");
    uint64_t storage_max_bytes = global_context->getConfigRef().getUInt64("storage_max_bytes", 0);
    if (storage_max_bytes && disk_info->total_bytes_on_disk > storage_max_bytes)
    {
        LOG_ERROR(log, "Disk usage is {} bytes, exceeds the limit {} bytes", disk_info->total_bytes_on_disk, storage_max_bytes);
        throw Exception(
            ErrorCodes::DISK_SIZE_EXCEED,
            "Disk usage is {} bytes, exceeds the limit {} bytes",
            disk_info->total_bytes_on_disk,
            storage_max_bytes);
    }
}

void PlacementService::preShutdown()
{
    if (broadcast_task)
        broadcast_task->deactivate();
}

void PlacementService::processRecords(const nlog::RecordPtrs & records)
{
    /// Node metrics schema: node, disk_free
    for (const auto & record : records)
    {
        assert(!record->hasSchema());
        assert(record->opcode() == nlog::OpCode::ADD_DATA_BLOCK);

        if (!record->hasIdempotentKey())
        {
            LOG_ERROR(log, "Invalid metric record, missing idempotent key");
            continue;
        }

        if (!record->hasHeader("_version"))
        {
            LOG_ERROR(log, "Invalid metric record, missing _version key");
            continue;
        }

        if (record->getHeader("_version") == "1")
        {
            mergeMetrics(record->idempotentKey(), *record);
        }
        else
        {
            LOG_ERROR(log, "Invalid version={} in metric record", record->getHeader("_version"));
        }
    }
}

void PlacementService::mergeMetrics(const String & node_identity, const nlog::Record & record)
{
    NodeMetricsPtr node_metrics = std::make_shared<NodeMetrics>(node_identity, record.getHeaders());
    if (!node_metrics->isValid() || node_metrics->node.channel.empty())
    {
        LOG_ERROR(log, "Invalid metric record: {}", node_metrics->node.string());
        return;
    }

    const auto & block = record.getBlock();

    DiskSpace disk_space;
    disk_space.reserve(block.rows());

    for (size_t row = 0, rows = block.rows(); row < rows; ++row)
    {
        const auto & policy_name = block.getByName("policy_name").column->getDataAt(row);
        const auto space = block.getByName("disk_space").column->get64(row);
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
        LOG_INFO(log, "Node identity={} host={} recovered from staleness", node_metrics->node.identity, node_metrics->node.host);
}

void PlacementService::scheduleBroadcast()
{
    if (!global_context->isDistributedEnv())
    {
        return;
    }

    /// Schedule the broadcast task
    broadcast_task = global_context->getSchedulePool().createTask("PlacementBroadcast", [this]() { this->broadcast(); });
    broadcast_task->activate();
    broadcast_task->schedule();
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

    broadcast_task->scheduleAfter(reschedule_interval);
}

void PlacementService::doBroadcast()
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get("string", nullptr);
    auto uint64_type = data_type_factory.get("uint64", nullptr);

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

    nlog::Record record{nlog::OpCode::ADD_DATA_BLOCK, std::move(disk_block), nlog::NO_SCHEMA};
    record.setShard(0);
    setupRecordHeaders(record, "1");

    const String table_count_query
        = "SELECT count(*) as table_counts FROM system.tables WHERE database != 'system' SETTINGS _tp_internal_system_open_sesame=true";

    auto context = Context::createCopy(global_context);
    context->makeQueryContext();
    executeNonInsertQuery(table_count_query, context, [&record](Block && block) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        const auto & table_counts_col = block.findByName("table_counts")->column;
        record.addHeader("_tables", std::to_string(table_counts_col->getUInt(0)));
    });

    record.addHeader("_broadcast_time", std::to_string(UTCMilliseconds::now()));

    const auto & result = appendRecord(record);
    if (result.err != ErrorCodes::OK)
    {
        /// FIXME: Error handling
        LOG_ERROR(log, "Failed to append node metrics error={}", result.err);
        return;
    }

    LOG_DEBUG(log, "Appended node metrics");
}

void PlacementService::loadLocalStoragesInfo(StreamStorageInfoPtr & storage_info, const String & ns, const String & stream) const
{
    const auto access = global_context->getAccess();
    if (stream.empty())
    {
        /// List all storages [in the specified database]
        Databases databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & [database_name, database] : databases)
        {
            /// Check whether is the specified database
            if (!ns.empty() && ns != database_name)
                continue;

            /// Check if database can contain MergeTree tables,
            /// if not it's unnecessary to load all tables of database just to filter all of them.
            if (!database->canContainMergeTreeTables())
                continue;

            for (auto iterator = database->getTablesIterator(global_context); iterator->isValid(); iterator->next())
            {
                StoragesInfo info;
                info.database = database_name;
                info.table = iterator->name();
                info.storage = iterator->table();
                if (!info.storage)
                    continue;

                if (!dynamic_cast<MergeTreeData *>(info.storage.get()))
                    continue;

                // if (!access->isGranted(AccessType::SHOW_TABLES, info.database, info.table))
                //     continue;

                StorageID storage_id{info.database, info.table};
                auto & stream_info = storage_info->streams[storage_id.getFullNameNotQuoted()];
                stream_info.id = storage_id;
                std::tie(stream_info.historical_data_bytes, stream_info.historical_data_paths) = getLocalStorageInfo(info.storage);
                storage_info->total_bytes_on_disk += stream_info.historical_data_bytes;
            }
        }
    }
    else
    {
        assert(!ns.empty());
        StorageID storage_id{ns, stream};
        auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, global_context);
        if (!storage)
            throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Not found '{}'.", storage_id.getNameForLogs());

        // if (!access->isGranted(AccessType::SHOW_TABLES, ns, stream))
        //     throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to show the stream '{}'", storage_id.getNameForLogs());

        auto & stream_info = storage_info->streams[storage_id.getFullNameNotQuoted()];
        stream_info.id = storage_id;

        if (!dynamic_cast<MergeTreeData *>(storage.get()))
            return; /// skip
        std::tie(stream_info.historical_data_bytes, stream_info.historical_data_paths) = getLocalStorageInfo(storage);
        storage_info->total_bytes_on_disk += stream_info.historical_data_bytes;
    }
}

std::pair<size_t, std::vector<String>> PlacementService::getLocalStorageInfo(StoragePtr storage) const
{
    assert(storage);
    StoragesInfo info;
    auto storage_id = storage->getStorageID();
    info.database = storage_id.database_name;
    info.table = storage_id.table_name;
    info.storage = storage;
    info.need_inactive_parts = true; /// or false?

    /// For table not to be dropped and set of columns to remain constant.
    info.table_lock
        = info.storage->lockForShare(global_context->getCurrentQueryId(), global_context->getSettingsRef().lock_acquire_timeout);

    info.engine = info.storage->getName();

    uint64_t total_size = 0;
    std::vector<String> paths;
    if (info.engine == "Stream")
    {
        const auto & stream = assert_cast<const StorageStream &>(*info.storage);
        const auto & stream_shards = stream.getStreamShards();
        for (const auto & stream_shard : stream_shards)
        {
            info.data = stream_shard->getStorage();
            if (!info.data)
                continue; /// virtual storage, it's a remote shard

            /// Iterator all parts
            MergeTreeData::DataPartStateVector all_parts_state;
            const auto & all_parts = info.getParts(all_parts_state, /*has_state_column*/ true);
            for (const auto & part : all_parts)
                total_size += part->getBytesOnDisk();

            const auto & shard_paths = info.data->getDataPaths();
            paths.insert(paths.end(), shard_paths.begin(), shard_paths.end());
        }
    }
    else if (auto * data = dynamic_cast<MergeTreeData *>(info.storage.get()))
    {
        info.data = data;
        MergeTreeData::DataPartStateVector all_parts_state;
        const auto & all_parts = info.getParts(all_parts_state, /*has_state_column*/ true);
        for (const auto & part : all_parts)
            total_size += part->getBytesOnDisk();

        paths = info.data->getDataPaths();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown engine {}", info.engine);


    return std::pair{total_size, std::move(paths)};
}
}
