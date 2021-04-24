#include "CatalogService.h"

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <regex>


namespace DB
{

/// From DatabaseOnDisk.h
std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextPtr context,
    bool has_force_restore_data_flag);

namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
/// Globals
const String CATALOG_KEY_PREFIX = "cluster_settings.system_catalogs.";
const String CATALOG_NAME_KEY = CATALOG_KEY_PREFIX + "name";
const String CATALOG_REPLICATION_FACTOR_KEY = CATALOG_KEY_PREFIX + "replication_factor";
const String CATALOG_DATA_RETENTION_KEY = CATALOG_KEY_PREFIX + "data_retention";
const String CATALOG_DEFAULT_TOPIC = "__system_catalogs";

const String THIS_HOST = getFQDNOrHostName();

Int32 parseShard(const String & engine_full)
{
    /// shard = <shard_number>
    static std::regex shard_regex("shard\\s*=\\s*(\\d+)");

    std::smatch shard_match;

    auto m = std::regex_search(engine_full, shard_match, shard_regex);
    assert(m);
    (void)m;

    return std::stoi(shard_match.str(1));
}
}

CatalogService & CatalogService::instance(const ContextPtr & context)
{
    static CatalogService catalog{context};
    return catalog;
}


CatalogService::CatalogService(const ContextPtr & global_context_) : MetadataService(global_context_, "CatalogService")
{
}

MetadataService::ConfigSettings CatalogService::configSettings() const
{
    return {
        .name_key = CATALOG_NAME_KEY,
        .default_name = CATALOG_DEFAULT_TOPIC,
        .data_retention_key = CATALOG_DATA_RETENTION_KEY,
        .default_data_retention = -1,
        .replication_factor_key = CATALOG_REPLICATION_FACTOR_KEY,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

void CatalogService::broadcast()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    try
    {
        doBroadcast();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to execute table query, error={}", getCurrentExceptionMessage(true, true));
    }
}

void CatalogService::doBroadcast()
{
    assert(dwal);

    /// Default max_block_size is 65505 (rows) which shall be bigger enough for a block to contain
    /// all tables on a single node
    String query = "SELECT * FROM system.tables WHERE (database != 'system') OR (database = 'system' AND name='tables')";

    /// FIXME, QueryScope
    /// CurrentThread::attachQueryContext(context);
    auto context = Context::createCopy(global_context);
    context->makeQueryContext();
    BlockIO io{executeQuery(query, context, true /* internal */)};

    if (io.pipeline.initialized())
    {
        processQueryWithProcessors(io.pipeline);
    }
    else if (io.in)
    {
        processQuery(io.in);
    }
    else
    {
        assert(false);
        LOG_ERROR(log, "Failed to execute table query");
    }
}

void CatalogService::processQueryWithProcessors(QueryPipeline & pipeline)
{
    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;

    while (executor.pull(block, 100))
    {
        if (block)
        {
            append(std::move(block));
            assert(!block);
        }
    }
}

void CatalogService::processQuery(BlockInputStreamPtr & in)
{
    AsynchronousBlockInputStream async_in(in);
    async_in.readPrefix();

    while (true)
    {
        if (async_in.poll(100))
        {
            Block block{async_in.read()};
            if (!block)
            {
                break;
            }

            append(std::move(block));
            assert(!block);
        }
    }

    async_in.readSuffix();
}

void CatalogService::append(Block && block)
{
    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block)};
    record.partition_key = 0;
    record.setIdempotentKey(global_context->getNodeIdentity());
    record.headers["_host"] = THIS_HOST;
    record.headers["_http_port"] = global_context->getConfigRef().getString("http_port", "8123");
    record.headers["_tcp_port"] = global_context->getConfigRef().getString("tcp_port", "9000");
    record.headers["_version"] = "1";

    /// FIXME : reschedule
    int retries = 3;
    while (retries--)
    {
        const auto & result = dwal->append(record, dwal_append_ctx);
        if (result.err == ErrorCodes::OK)
        {
            LOG_INFO(log, "Appended {} table definitions in one block", record.block.rows());
            return;
        }

        LOG_ERROR(log, "Failed to append table definition block, error={}", result.err);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
}

std::vector<String> CatalogService::databases() const
{
    std::unordered_set<String> dbs;

    {
        std::shared_lock guard{catalog_rwlock};
        for (const auto & p : indexed_by_name)
        {
            dbs.insert(p.first.first);
        }
    }

    return std::vector<String>{dbs.begin(), dbs.end()};
}

CatalogService::TablePtrs CatalogService::tables() const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    for (const auto & p : indexed_by_name)
    {
        for (const auto & pp : p.second)
        {
            results.push_back(pp.second);
        }
    }
    return results;
}

CatalogService::TablePtrs CatalogService::findTableByNode(const String & node_identity) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    auto iter = indexed_by_node.find(node_identity);
    if (iter != indexed_by_node.end())
    {
        for (const auto & p : iter->second)
        {
            results.push_back(p.second);
        }
    }
    return results;
}

StoragePtr CatalogService::createVirtualTableStorage(const String & query, const String & database, const String & table)
{
    auto database_table = std::make_pair(database, table);

    UUID uuid;
    /// Check if the database, table exists in `indexed_by_name` first
    {
        std::shared_lock guard{catalog_rwlock};
        auto iter = indexed_by_name.find(database_table);
        if (iter == indexed_by_name.end() || iter->second.empty())
        {
            return nullptr;
        }

        /// Only support create virtual table storage for `DistributedMergeTree`
        if (iter->second.begin()->second->engine != "DistributedMergeTree")
        {
            return nullptr;
        }

        uuid = iter->second.begin()->second->uuid;
        assert(uuid != UUIDHelpers::Nil);
    }

    auto settings = global_context->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    String error_message;

    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
            "from catalog_service", /* allow_multi_statements = */ false, 0, settings.max_parser_depth);

    if (!ast)
    {
        LOG_ERROR(log, "Failed to parse table creation query={} error={}", query, error_message);
        return nullptr;
    }

    /// `table_data_path_relative = ""` means virtual. There is no data / metadata bound to
    /// the storage engine
    auto [table_name, storage]
        = createTableFromAST(ast->as<ASTCreateQuery &>(), database, /* table_data_path_relative = */ "", global_context, false);

    /// Setup UUID
    auto storage_id = storage->getStorageID();
    storage_id.uuid = uuid;
    storage->renameInMemory(storage_id);

    if (likely(setTableStorageByName(database, table, storage)))
    {
        return storage;
    }
    else
    {
        LOG_INFO(log, "Table was deleted during virtual table storage creation, database={} table={}", database, table);
        return nullptr;
    }
}

bool CatalogService::setTableStorageByName(const String & database, const String & table, const StoragePtr & storage)
{
    auto database_table = std::make_pair(database, table);

    /// Check if the database, table exists in `indexed_by_name` first
    {
        std::shared_lock guard{catalog_rwlock};
        auto iter = indexed_by_name.find(database_table);
        if (iter == indexed_by_name.end() || iter->second.empty())
        {
            return false;
        }
    }

    {
        std::unique_lock storage_guard{storage_rwlock};
        storages[std::make_pair(database, table)] = storage;
    }
    return true;
}

std::pair<CatalogService::TablePtr, StoragePtr> CatalogService::findTableStorageById(const UUID & uuid) const
{
    std::shared_lock storage_guard{storage_rwlock};

    auto iter = indexed_by_id.find(uuid);
    if (iter != indexed_by_id.end())
    {
        auto storage_iter = storages.find(std::make_pair(iter->second->database, iter->second->name));
        if (storage_iter != storages.end())
        {
            return {iter->second, storage_iter->second};
        }
        return {iter->second, nullptr};
    }
    return {};
}

std::pair<CatalogService::TablePtr, StoragePtr> CatalogService::findTableStorageByName(const String & database, const String & table) const
{
    auto database_table = std::make_pair(database, table);
    TablePtr table_p;
    {

        std::shared_lock guard{catalog_rwlock};
        auto iter = indexed_by_name.find(database_table);
        if (iter == indexed_by_name.end() || iter->second.empty())
        {
            return {};
        }
        table_p = iter->second.begin()->second;
    }

    std::shared_lock storage_guard{storage_rwlock};
    auto storage_iter = storages.find(database_table);
    if (storage_iter != storages.end())
    {
        return {table_p, storage_iter->second};
    }
    return {table_p, nullptr};
}

CatalogService::TablePtrs CatalogService::findTableByName(const String & database, const String & table) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    auto iter = indexed_by_name.find(std::make_pair(database, table));
    if (iter != indexed_by_name.end())
    {
        for (const auto & p : iter->second)
        {
            results.push_back(p.second);
        }
    }
    return results;
}

CatalogService::TablePtrs CatalogService::findTableByDB(const String & database) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    for (const auto & p : indexed_by_name)
    {
        if (p.first.first == database)
        {
            for (const auto & pp : p.second)
            {
                results.push_back(pp.second);
            }
        }
    }

    return results;
}

bool CatalogService::tableExists(const String & database, const String & table) const
{
    std::shared_lock guard{catalog_rwlock};

    return indexed_by_name.find(std::make_pair(database, table)) != indexed_by_name.end();
}

ClusterPtr CatalogService::tableCluster(const String & database, const String & table, Int32 replication_factor, Int32 shards)
{
    /// FIXME : pick the right replica to service the query request
    auto key = std::make_pair(database, table);
    {
        std::shared_lock guard{table_cluster_rwlock};

        auto iter = table_clusters.find(key);
        if (iter != table_clusters.end())
        {
            return iter->second;
        }
    }

    TablePtrs remote_tables{findTableByName(database, table)};
    if (remote_tables.empty())
    {
        LOG_WARNING(log, "Missing database={} table={} in CatalogService, can't build cluster", database, table);
        return nullptr;
    }

    if (replication_factor * shards != static_cast<Int32>(remote_tables.size()))
    {
        LOG_WARNING(
            log,
            "Missing total shard replicas, expected={} got={} for database={} table={}",
            replication_factor * shards,
            remote_tables.size(),
            database,
            table);
    }

    /// Group table by shard ID
    std::unordered_map<Int32, TablePtrs> by_shard;
    for (auto & table_ptr : remote_tables)
    {
        by_shard[table_ptr->shard].push_back(table_ptr);
    }

    std::vector<std::vector<String>> host_ports;
    for (const auto & shard_replicas : by_shard)
    {
        /// FIXME : when the node is back online, we shall refresh the table cluster
        if (static_cast<Int32>(shard_replicas.second.size()) != replication_factor)
        {
            LOG_WARNING(
                log, "Missing replicas, expected={} got={} for database={} table={} shard={}", database, table, shard_replicas.first);
        }

        std::shared_lock guard{catalog_rwlock};

        std::vector<String> replica_hosts;
        for (const auto & table_ptr : shard_replicas.second)
        {
            auto iter = table_nodes.find(table_ptr->node_identity);
            if (iter == table_nodes.end())
            {
                LOG_ERROR(log, "Missing node identity for node={}", table_ptr->node_identity);
                continue;
            }

            replica_hosts.push_back(iter->second->host + ":" + std::to_string(iter->second->tcp_port));
        }
        host_ports.push_back(std::move(replica_hosts));
        assert(replica_hosts.empty());
    }

    /// FIXME, user/password etc
    auto table_cluster = std::make_shared<Cluster>(
        global_context->getSettingsRef(),
        host_ports,
        /* username = */ "",
        /* password = */ "",
        9000,
        /* treat_local_as_remote = */ false,
        /* secure = */ false,
        /* priority = */ 1);

    {
        std::unique_lock guard{table_cluster_rwlock};
        table_clusters[key] = table_cluster;
    }
    return table_cluster;
}

CatalogService::TableContainerPerNode CatalogService::buildCatalog(const NodePtr & node, const Block & block)
{
    TableContainerPerNode snapshot;

    for (size_t row = 0; row < block.rows(); ++row)
    {
        TablePtr table = std::make_shared<Table>(node->identity, node->host);
        std::unordered_map<String, void *> kvp = {
            {"database", &table->database},
            {"name", &table->name},
            {"engine", &table->engine},
            {"uuid", &table->uuid},
            {"metadata_path", &table->metadata_path},
            {"data_paths", &table->data_paths},
            {"dependencies_database", &table->dependencies_database},
            {"dependencies_table", &table->dependencies_table},
            {"create_table_query", &table->create_table_query},
            {"engine_full", &table->engine_full},
            {"partition_key", &table->partition_key},
            {"sorting_key", &table->sorting_key},
            {"primary_key", &table->primary_key},
            {"sampling_key", &table->sampling_key},
            {"storage_policy", &table->storage_policy},
            {"total_rows", &table->total_rows},
            {"total_bytes", &table->total_bytes},
        };

        for (const auto & col : block)
        {
            auto it = kvp.find(col.name);
            if (it != kvp.end())
            {
                if (col.name == "total_rows" || col.name == "total_bytes")
                {
                    *static_cast<UInt64 *>(it->second) = col.column->get64(row);
                }
                else if (col.name == "data_paths" || col.name == "dependencies_database" || col.name == "dependencies_table")
                {
                    /// String array
                    WriteBufferFromOwnString buffer;
                    col.type->getDefaultSerialization()->serializeText(*col.column, row, buffer, FormatSettings{});
                    *static_cast<String *>(it->second) = buffer.str();
                }
                else if (col.name == "uuid")
                {
                    *static_cast<UUID *>(it->second) = static_cast<const ColumnUInt128 *>(col.column.get())->getElement(row);
                }
                else
                {
                    /// String
                    *static_cast<String *>(it->second) = col.column->getDataAt(row).toString();
                }
            }
        }

        if (table->engine == "DistributedMergeTree")
        {
            table->shard = parseShard(table->engine_full);
        }

        if (table->database == "system")
        {
            /// Ignore tables in `system` database
            continue;
        }

        DatabaseTableShard key = std::make_pair(table->database, std::make_pair(table->name, table->shard));
        snapshot.emplace(std::move(key), std::move(table));

        assert(!table);
    }

    LOG_INFO(log, "Got {} tables from host={} identity={}", snapshot.size(), node->host, node->identity);

    return snapshot;
}

/// Merge a snapshot of tables from one host
void CatalogService::mergeCatalog(const NodePtr & node, TableContainerPerNode snapshot)
{
    if (snapshot.empty())
    {
        return;
    }

    std::unique_lock guard{catalog_rwlock};

    auto iter = indexed_by_node.find(node->identity);
    if (iter == indexed_by_node.end())
    {
        /// New host. Add all tables from this host to `indexed_by_name`
        for (const auto & p : snapshot)
        {
            assert(p.second->uuid != UUIDHelpers::Nil);

            indexed_by_name[std::make_pair(p.second->database, p.second->name)].emplace(
                std::make_pair(p.second->node_identity, p.second->shard), p.second);

            {
                std::unique_lock storage_guard{storage_rwlock};
                assert(!indexed_by_id.contains(p.second->uuid));
                indexed_by_id[p.second->uuid] = p.second;
            }
        }
        indexed_by_node.emplace(node->identity, snapshot);

        /// This is a new node
        /// FIXME : support tcp/http port changes and dead node
        auto res = table_nodes.emplace(node->identity, node);
        assert(res.second);
        (void)res;
        return;
    }

    /// Found node. Merge existing tables from this node to `indexed_by_name`
    /// and delete `deleted` table entries from `indexed_by_name`
    for (const auto & p : iter->second)
    {
        /// ((database, tablename, shard), table) pair
        if (!snapshot.contains(p.first))
        {
            auto iter_by_name = indexed_by_name.find(std::make_pair(p.second->database, p.second->name));
            assert(iter_by_name != indexed_by_name.end());

            /// Deleted table, remove from `indexed_by_name` and `indexed_by_id`
            auto removed = iter_by_name->second.erase(std::make_pair(p.second->node_identity, p.second->shard));
            assert(removed == 1);
            (void)removed;

            if (iter_by_name->second.empty())
            {
                indexed_by_name.erase(iter_by_name);
            }

            {
                std::unique_lock storage_guard{storage_rwlock};
                removed = indexed_by_id.erase(p.second->uuid);
                assert(removed == 1);
                (void)removed;
            }
        }
    }

    /// Add new tables or override existing tables in `indexed_by_name`
    for (const auto & p : snapshot)
    {
        UUID uuid = UUIDHelpers::Nil;
        auto node_shard =  std::make_pair(p.second->node_identity, p.second->shard);

        DatabaseTable db_table = std::make_pair(p.second->database, p.second->name);
        auto iter_by_name = indexed_by_name.find(db_table);
        if (iter_by_name == indexed_by_name.end())
        {
            /// New table
            indexed_by_name[db_table].emplace(std::move(node_shard), p.second);
        }
        else
        {
            /// An existing table
            auto node_shard_iter = iter_by_name->second.find(node_shard);
            if (node_shard_iter != iter_by_name->second.end() && node_shard_iter->second->uuid != p.second->uuid)
            {
                /// If uuid changed (table with same name got deleted and recreatd), delete it from indexed_by_id
                uuid = node_shard_iter->second->uuid;
            }
            iter_by_name->second.insert_or_assign(std::move(node_shard), p.second);
        }

        {
            /// FIXME, if table definition changed, we will need update the storage inline
            std::unique_lock storage_guard{storage_rwlock};
            if (uuid != UUIDHelpers::Nil)
            {
                auto removed = indexed_by_id.erase(uuid);
		(void)removed;
                assert(removed);
                assert(removed == 1);
            }
            indexed_by_id[p.second->uuid] = p.second;
        }
    }

    /// Replace all tables for this host in `indexed_by_node`
    indexed_by_node[node->identity].swap(snapshot);
}

void CatalogService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);

        if (!record->hasIdempotentKey())
        {
            LOG_ERROR(log, "Invalid catalog block, missing identity");
            continue;
        }

        auto iter = record->headers.find("_version");
        if (iter == record->headers.end() || iter->second != "1")
        {
            LOG_ERROR(log, "Invalid version or missing version");
            continue;
        }

        auto node = std::make_shared<Node>(record->idempotentKey(), record->headers);
        if (!node->isValid())
        {
            LOG_WARNING(log, "Invalid catalog block, invalid headers={}", node->string());
            continue;
        }

        mergeCatalog(node, buildCatalog(node, record->block));
    }
}
}
