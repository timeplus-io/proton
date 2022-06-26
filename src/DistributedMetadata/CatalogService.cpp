#include "CatalogService.h"
#include "queryStreams.h"

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/Streaming/parseHostShards.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <regex>


namespace DB
{
/// From DatabaseOnDisk.h
std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextMutablePtr context,
    bool has_force_restore_data_flag);

namespace ErrorCodes
{
    extern const int OK;
    extern const int NO_SUCH_COLUMN_IN_STREAM;
}

namespace
{
    /// Globals
    const String CATALOG_KEY_PREFIX = "cluster_settings.system_catalogs.";
    const String CATALOG_DEFAULT_TOPIC = "__system_catalogs";

    std::regex PARSE_HOST_SHARDS_REGEX{"host_shards\\s*=\\s*'([,|\\s|\\d]*)'"};
    std::regex PARSE_SHARDS_REGEX{"Stream\\(\\s*(\\d+),\\s*\\d+\\s*,"};
    std::regex PARSE_REPLICATION_REGEX{"Stream\\(\\s*\\d+,\\s*(\\d+)\\s*,"};

    Int32 searchIntValueByRegex(const std::regex & pattern, const String & str)
    {
        std::smatch pattern_match;

        auto m = std::regex_search(str, pattern_match, pattern);
        /// assert(m);
        /// (void)m;
        /// FIXME, without ddl/placement service (nativelog case), we will not assign shard number
        if (!m)
            return 0;

        return std::stoi(pattern_match.str(1));
    }
}

CatalogService & CatalogService::instance(const ContextMutablePtr & context)
{
    static CatalogService catalog{context};
    return catalog;
}

CatalogService::CatalogService(const ContextMutablePtr & global_context_) : MetadataService(global_context_, "CatalogService")
{
}

MetadataService::ConfigSettings CatalogService::configSettings() const
{
    return {
        .key_prefix = CATALOG_KEY_PREFIX,
        .default_name = CATALOG_DEFAULT_TOPIC,
        .default_data_retention = -1,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

void CatalogService::broadcast()
{
    if (!global_context->isDistributedEnv())
        return;

    try
    {
        doBroadcast();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to execute query, error={}", getCurrentExceptionMessage(true, true));
    }
}

void CatalogService::doBroadcast()
{
    assert(dwal);

    auto query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    /// CurrentThread::QueryScope query_scope{query_context};

    queryStreams(query_context, [this](Block && block) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        append(std::move(block));
        assert(!block);
    });
}

void CatalogService::append(Block && block)
{
    nlog::Record record{nlog::OpCode::ADD_DATA_BLOCK, std::move(block), nlog::NO_SCHEMA};
    record.setShard(0);
    setupRecordHeaders(record, "1");

    const auto & result = appendRecord(record);
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to appended {} tables to DWAL", record.getBlock().rows());
        return;
    }

    LOG_INFO(log, "Appended {} stream definitions in one block", record.getBlock().rows());
}

std::vector<String> CatalogService::databases() const
{
    std::unordered_set<String> dbs;

    {
        std::shared_lock guard{catalog_rwlock};
        for (const auto & p : indexed_by_name)
            dbs.insert(p.first.first);
    }

    return std::vector<String>{dbs.begin(), dbs.end()};
}

CatalogService::TablePtrs CatalogService::tables() const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    for (const auto & p : indexed_by_name)
        for (const auto & pp : p.second)
            results.push_back(pp.second);

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
            results.push_back(p.second);
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
            return nullptr;

        /// Only support create virtual table storage for `Stream`
        if (iter->second.begin()->second->engine != "Stream")
            return nullptr;

        uuid = iter->second.begin()->second->uuid;
        assert(uuid != UUIDHelpers::Nil);
    }

    auto settings = global_context->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    String error_message;

    auto ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "from catalog_service",
        /* allow_multi_statements = */ false,
        0,
        settings.max_parser_depth);

    if (!ast)
    {
        LOG_ERROR(log, "Failed to parse stream creation query={} error={}", query, error_message);
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
        storage->startup();
        return storage;
    }
    else
    {
        LOG_INFO(log, "Stream was deleted during virtual stream storage creation, database={} table={}", database, table);
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
            return false;
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
            return {iter->second, storage_iter->second};

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
            return {};

        table_p = iter->second.begin()->second;
    }

    std::shared_lock storage_guard{storage_rwlock};
    auto storage_iter = storages.find(database_table);
    if (storage_iter != storages.end())
        return {table_p, storage_iter->second};

    return {table_p, nullptr};
}

void CatalogService::deleteTableStorageByName(const String & database, const String & table)
{
    deleteTableStorageByName({database, table});
}

inline void CatalogService::deleteTableStorageByName(const DatabaseTable & database_table)
{
    std::unique_lock storage_guard{storage_rwlock};
    storages.erase(database_table);
}

CatalogService::TablePtrs CatalogService::findTableByName(const String & database, const String & table) const
{
    return findTableByName({database, table});
}

inline CatalogService::TablePtrs CatalogService::findTableByName(const DatabaseTable & database_table) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard{catalog_rwlock};

    auto iter = indexed_by_name.find(database_table);
    if (iter != indexed_by_name.end())
    {
        for (const auto & p : iter->second)
            results.push_back(p.second);
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
                results.push_back(pp.second);
        }
    }

    return results;
}

bool CatalogService::tableExists(const String & database, const String & table) const
{
    std::shared_lock guard{catalog_rwlock};

    return indexed_by_name.find(std::make_pair(database, table)) != indexed_by_name.end();
}

std::pair<bool, bool> CatalogService::columnExists(const String & database, const String & table, const String & column) const
{
    const auto & tables = findTableByName(database, table);

    if (tables.empty())
        return {false, false};

    const auto & query_ptr = parseQuery(tables[0]->create_table_query, global_context);
    const auto & create = query_ptr->as<const ASTCreateQuery &>();
    const auto & columns_ast = create.columns_list->columns;

    for (auto ast_it = columns_ast->children.begin(); ast_it != columns_ast->children.end(); ++ast_it)
    {
        const auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();
        if (col_decl.name == column)
            return {true, true};
    }

    return {true, false};
}

String CatalogService::getColumnType(const String & database, const String & table, const String & column) const
{
    const auto & tables = findTableByName(database, table);
    const auto & query_ptr = parseQuery(tables[0]->create_table_query, global_context);
    const auto & create = query_ptr->as<const ASTCreateQuery &>();
    const auto & columns_ast = create.columns_list->columns;

    for (auto ast_it = columns_ast->children.begin(); ast_it != columns_ast->children.end(); ++ast_it)
    {
        const auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();
        if (col_decl.name == column)
            return queryToString(col_decl.type);
    }

    throw Exception("Could not found the column : " + column + " in stream : " + table, ErrorCodes::NO_SUCH_COLUMN_IN_STREAM);
}

void CatalogService::deleteCatalogForNode(const NodePtr & node)
{
    std::unique_lock guard{catalog_rwlock};

    auto iter = indexed_by_node.find(node->identity);
    if (iter == indexed_by_node.end())
        return;

    for (const auto & p : iter->second)
    {
        auto iter_by_name = indexed_by_name.find(p.first);
        assert(iter_by_name != indexed_by_name.end());

        deleteTableStorageByName(p.first);

        /// Deleted table, remove t from `indexed_by_name` and `indexed_by_id`
        [[maybe_unused]] auto removed = iter_by_name->second.erase(p.second->node_identity);
        assert(removed == 1);

        if (iter_by_name->second.empty())
            indexed_by_name.erase(iter_by_name);

        {
            std::unique_lock storage_guard{storage_rwlock};
            removed = indexed_by_id.erase(p.second->uuid);
            assert(removed == 1);
        }
    }

    /// After clear, we don't remove this `node` from `indexed_by_node` map
    iter->second.clear();
}

std::vector<NodePtr> CatalogService::nodes(const String & role) const
{
    std::vector<NodePtr> result;
    {
        std::shared_lock guard{catalog_rwlock};
        result.reserve(cluster_nodes.size());

        for (const auto & idem_node : cluster_nodes)
        {
            if (idem_node.second->roles.find(role) != String::npos)
                result.push_back(idem_node.second);
        }
    }
    return result;
}

NodePtr CatalogService::nodeByIdentity(const String & identity) const
{
    std::shared_lock guard{catalog_rwlock};

    auto iter = cluster_nodes.find(identity);
    if (iter != cluster_nodes.end())
        return iter->second;

    return nullptr;
}

NodePtr CatalogService::nodeByChannel(const String & channel) const
{
    std::shared_lock guard(catalog_rwlock);

    for (const auto & node : cluster_nodes)
    {
        if (node.second->channel == channel)
            return node.second;
    }

    return nullptr;
}

ClusterPtr CatalogService::tableCluster(const String & database, const String & table, Int32 replication_factor, Int32 shards)
{
    /// FIXME : pick the right replica to service the query request
    auto key = std::make_pair(database, table);
    {
        std::shared_lock guard{table_cluster_rwlock};

        auto iter = table_clusters.find(key);
        if (iter != table_clusters.end())
            return iter->second;
    }

    TablePtrs remote_tables{findTableByName(key)};
    if (remote_tables.empty())
    {
        LOG_WARNING(log, "Missing database={} table={} in CatalogService, can't build cluster", database, table);
        return nullptr;
    }

    Int32 total_replicas = 0;
    for (const auto & t : remote_tables)
        total_replicas += t->host_shards.size();

    if (replication_factor * shards != total_replicas)
    {
        LOG_WARNING(
            log,
            "Missing total shard replicas, expected={} got={} for database={} table={}",
            replication_factor * shards,
            total_replicas,
            database,
            table);
    }

    /// Group table by shard ID
    std::unordered_map<Int32, TablePtrs> by_shard;
    for (auto & table_ptr : remote_tables)
        for (auto shard : table_ptr->host_shards)
            by_shard[shard].push_back(table_ptr);

    std::vector<std::vector<String>> host_ports;
    for (const auto & shard_replicas : by_shard)
    {
        /// FIXME : when the node is back online, we shall refresh the table cluster
        if (static_cast<Int32>(shard_replicas.second.size()) != replication_factor)
        {
            LOG_WARNING(
                log,
                "Missing replicas, expected={} got={} for database={} table={} shard={}",
                shard_replicas.second.size(),
                replication_factor,
                database,
                table,
                shard_replicas.first);
        }

        std::shared_lock guard{catalog_rwlock};

        std::vector<String> replica_hosts;
        for (const auto & table_ptr : shard_replicas.second)
        {
            auto iter = cluster_nodes.find(table_ptr->node_identity);
            if (iter == cluster_nodes.end())
            {
                LOG_ERROR(log, "Missing node in cluster for node_identity={}", table_ptr->node_identity);
                continue;
            }

            replica_hosts.push_back(fmt::format("{}:{}", iter->second->host, iter->second->tcp_port));
        }

        host_ports.push_back(std::move(replica_hosts));
    }

    /// FIXME, user/password/https etc
    auto table_cluster = std::make_shared<Cluster>(
        global_context->getSettingsRef(),
        host_ports,
        /* username = */ "",
        /* password = */ "",
        tcp_port,
        /* treat_local_as_remote = */ false,
        /* secure = */ false,
        /* priority = */ 1);

    {
        std::unique_lock guard{table_cluster_rwlock};
        table_clusters[key] = table_cluster;
    }
    return table_cluster;
}

CatalogService::TableContainer CatalogService::buildCatalog(const NodePtr & node, const Block & block)
{
    TableContainer snapshot;
    snapshot.reserve(block.rows());

    for (size_t row = 0, rows = block.rows(); row < rows; ++row)
    {
        TablePtr table = std::make_shared<Table>(node->identity, node->host, block, row);
        DatabaseTable key = std::make_pair(table->database, table->name);
        snapshot.emplace(std::move(key), std::move(table));
    }

    LOG_INFO(log, "Got {} tables from host={} identity={}", snapshot.size(), node->host, node->identity);

    return snapshot;
}

/// Merge a snapshot of tables from one host
void CatalogService::mergeCatalog(const NodePtr & node, TableContainer snapshot)
{
    if (snapshot.empty())
        return deleteCatalogForNode(node);

    std::unique_lock guard{catalog_rwlock};

    auto iter = indexed_by_node.find(node->identity);
    if (iter == indexed_by_node.end())
    {
        /// New host. Add all tables from this host to `indexed_by_name`
        for (const auto & p : snapshot)
        {
            if (p.second->engine == "View" && p.second->uuid == UUIDHelpers::Nil)
                continue;

            indexed_by_name[p.first].emplace(p.second->node_identity, p.second);

            {
                std::unique_lock storage_guard{storage_rwlock};
                assert(!indexed_by_id.contains(p.second->uuid));
                indexed_by_id[p.second->uuid] = p.second;
            }
        }
        indexed_by_node.emplace(node->identity, snapshot);

        /// This is a new node
        [[maybe_unused]] auto res = cluster_nodes.emplace(node->identity, node);
        assert(res.second);
        return;
    }

    /// Existing node, update it
    /// FIXME : support tcp/http port changes and dead node
    cluster_nodes[node->identity] = node;

    /// Merge existing tables from this node to `indexed_by_name`
    /// and delete `deleted` table entries from `indexed_by_name`
    for (const auto & p : iter->second)
    {
        /// ((database, table), table_ptr) pair
        if (!snapshot.contains(p.first))
        {
            /// This is a deleted table since it doesn't exist in latest snapshot
            auto iter_by_name = indexed_by_name.find(p.first);
            assert(iter_by_name != indexed_by_name.end());

            deleteTableStorageByName(p.first);

            /// Deleted table, remove from `indexed_by_name` and `indexed_by_id`
            [[maybe_unused]] auto removed = iter_by_name->second.erase(p.second->node_identity);
            assert(removed == 1);

            if (iter_by_name->second.empty())
                indexed_by_name.erase(iter_by_name);

            {
                std::unique_lock storage_guard{storage_rwlock};
                removed = indexed_by_id.erase(p.second->uuid);
                assert(removed == 1);
            }
        }
    }

    /// Add new tables or override existing tables in `indexed_by_name`
    for (const auto & p : snapshot)
    {
        UUID uuid = UUIDHelpers::Nil;
        auto iter_by_name = indexed_by_name.find(p.first);
        if (iter_by_name == indexed_by_name.end())
        {
            /// New table
            indexed_by_name[p.first].emplace(node->identity, p.second);
        }
        else
        {
            /// An existing table on that node by name
            auto node_shard_iter = iter_by_name->second.find(node->identity);
            if (node_shard_iter != iter_by_name->second.end() && node_shard_iter->second->uuid != p.second->uuid)
                /// If uuid changed (table with same name got deleted and recreated), delete it from indexed_by_id
                uuid = node_shard_iter->second->uuid;

            /// if table definition changed , delete the storage
            if (node_shard_iter != iter_by_name->second.end() && *node_shard_iter->second != *p.second)
                deleteTableStorageByName(p.first);

            iter_by_name->second.insert_or_assign(node->identity, p.second);
        }

        {
            std::unique_lock storage_guard{storage_rwlock};
            if (uuid != UUIDHelpers::Nil)
            {
                [[maybe_unused]] auto removed = indexed_by_id.erase(uuid);
                assert(removed == 1);
            }
            indexed_by_id[p.second->uuid] = p.second;
        }
    }

    /// Replace all tables for this host in `indexed_by_node`
    indexed_by_node[node->identity].swap(snapshot);
}

void CatalogService::processRecords(const nlog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        assert(!record->hasSchema());
        assert(record->opcode() == nlog::OpCode::ADD_DATA_BLOCK);

        if (!record->hasIdempotentKey())
        {
            LOG_ERROR(log, "Invalid catalog block, missing identity");
            continue;
        }

        if (!record->hasHeader("_version"))
        {
            LOG_ERROR(log, "Invalid version or missing version");
            continue;
        }

        if (record->getHeader("_version") != "1")
        {
            LOG_ERROR(log, "Invalid version={} in catalog record", record->getHeader("_version"));
            continue;
        }

        auto node = std::make_shared<Node>(record->idempotentKey(), record->getHeaders());
        if (!node->isValid())
        {
            LOG_WARNING(log, "Invalid catalog block, invalid headers={}", node->string());
            continue;
        }

        mergeCatalog(node, buildCatalog(node, record->getBlock()));
    }
}

std::pair<Int32, Int32> CatalogService::shardAndReplicationFactor(const String & database, const String & table) const
{
    const auto & tables = findTableByName(database, table);

    if (tables.empty())
        return {0, 0};

    return {
        searchIntValueByRegex(PARSE_REPLICATION_REGEX, tables[0]->engine_full),
        searchIntValueByRegex(PARSE_SHARDS_REGEX, tables[0]->engine_full)};
}


CatalogService::Table::Table(const String & node_identity_, const String & host_, const Block & block, size_t row)
    : node_identity(node_identity_), host(host_)
{
    std::unordered_map<String, void *> kvp = {
        {"database", &database},
        {"name", &name},
        {"engine", &engine},
        {"uuid", &uuid},
        {"dependencies_table", &dependencies_table},
        {"create_table_query", &create_table_query},
        {"engine_full", &engine_full},
        {"partition_key", &partition_key},
        {"sorting_key", &sorting_key},
        {"primary_key", &primary_key},
        {"sampling_key", &sampling_key},
        {"storage_policy", &storage_policy},
    };

    for (const auto & col : block)
    {
        auto it = kvp.find(col.name);
        if (it != kvp.end())
        {
            if (col.name == "dependencies_table")
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

    if (engine == "Stream")
    {
        auto shards = searchIntValueByRegex(PARSE_SHARDS_REGEX, engine_full);

        std::smatch pattern_match;
        auto m = std::regex_search(engine_full, pattern_match, PARSE_HOST_SHARDS_REGEX);
        if (!m)
        {
            for (Int32 i = 0; i < shards; ++i)
                host_shards.push_back(i);
        }
        else
        {
            host_shards = parseHostShards(pattern_match.str(1), shards);
        }
    }
}

}
