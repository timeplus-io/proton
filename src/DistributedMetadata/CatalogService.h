#pragma once

#include "MetadataService.h"
#include "Node.h"

/// #include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/Cluster.h>
#include <QueryPipeline/QueryPipeline.h>

#include <boost/functional/hash.hpp>


namespace DB
{
class Context;

class CatalogService final : public MetadataService
{
public:
    struct Table
    {
        /// `node_identity` can be unique uuid
        String node_identity;
        /// Duplicate host here for conveniency. Host is an ip or hostname which
        /// can be reachable via network
        String host;

        String database;
        String name;
        UUID uuid = UUIDHelpers::Nil;
        String engine;
        String metadata_path;
        String data_paths;
        String dependencies_database;
        String dependencies_table;
        String create_table_query;
        String engine_full;
        String partition_key;
        String sorting_key;
        String primary_key;
        String sampling_key;
        String storage_policy;
        std::vector<Int32> host_shards;

        Table(const String & node_identity_, const String & host_, const Block & block, size_t rows);

        friend bool operator==(const Table & lhs, const Table & rhs)
        {
            bool host_shards_equals = std::equal(lhs.host_shards.begin(), lhs.host_shards.end(), rhs.host_shards.begin(), rhs.host_shards.end());

            return host_shards_equals && (lhs.node_identity == rhs.node_identity) && (lhs.host == rhs.host) && (lhs.database == rhs.database)
                && (lhs.name == rhs.name) && (lhs.uuid == rhs.uuid) && (lhs.engine == rhs.engine)
                && (lhs.metadata_path == rhs.metadata_path) && (lhs.data_paths == rhs.data_paths)
                && (lhs.dependencies_database == rhs.dependencies_database) && (lhs.dependencies_table == rhs.dependencies_table)
                && (lhs.create_table_query == rhs.create_table_query) && (lhs.engine_full == rhs.engine_full)
                && (lhs.partition_key == rhs.partition_key) && (lhs.sorting_key == rhs.sorting_key) && (lhs.primary_key == rhs.primary_key)
                && (lhs.sampling_key == rhs.sampling_key) && (lhs.storage_policy == rhs.storage_policy);
        }

        friend bool operator!=(const Table & lhs, const Table & rhs) { return !(lhs == rhs); }
    };

    using TablePtr = std::shared_ptr<Table>;
    using TablePtrs = std::vector<TablePtr>;

public:
    static CatalogService & instance(const ContextMutablePtr & context_);

    explicit CatalogService(const ContextMutablePtr & context_);
    virtual ~CatalogService() override = default;

    /// `broadcast` broadcasts the table catalog metadata on this node
    /// to all nodes with CatalogService role in the cluster via DWAL
    void broadcast();

    StoragePtr createVirtualTableStorage(const String & query, const String & database, const String & table);

    std::pair<TablePtr, StoragePtr> findTableStorageById(const UUID & uuid) const;
    std::pair<TablePtr, StoragePtr> findTableStorageByName(const String & database, const String & table) const;
    void deleteTableStorageByName(const String & database, const String & table);

    TablePtrs findTableByName(const String & database, const String & table) const;
    TablePtrs findTableByNode(const String & node_identity) const;
    TablePtrs findTableByDB(const String & database) const;

    bool tableExists(const String & database, const String & table) const;
    std::pair<bool, bool> columnExists(const String & database, const String & table, const String & column) const;
    String getColumnType(const String & database, const String & table, const String & column) const;

    TablePtrs tables() const;
    std::vector<String> databases() const;

    /// Return the nodes addresses, which host the shards of the database table
    ClusterPtr tableCluster(const String & database, const String & table, Int32 replication_factor, Int32 shards);

    void deleteCatalogForNode(const NodePtr & node);
    std::pair<Int32, Int32> shardAndReplicationFactor(const String & database, const String & table) const;

    std::vector<NodePtr> nodes(const String & role = "") const;
    NodePtr nodeByIdentity(const String & identity) const;
    NodePtr nodeByChannel(const String & channel) const;

private:
    bool setTableStorageByName(const String & database, const String & table, const StoragePtr & storage);

    void processRecords(const nlog::RecordPtrs & records) override;
    String role() const override { return "catalog"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(1000, 200); }

    TablePtrs findTableByName(const std::pair<String, String> & database_table) const;
    void deleteTableStorageByName(const std::pair<String, String> & database_table);

private:
    void doBroadcast();
    /// void processQuery(BlockInputStreamPtr & in);
    /// void processQueryWithProcessors(QueryPipeline & pipeline);
    void append(Block && block);

private:
    using DatabaseTable = std::pair<String, String>;

    using TableContainer = std::unordered_map<DatabaseTable, TablePtr, boost::hash<DatabaseTable>>;
    using StorageContainer = std::unordered_map<DatabaseTable, StoragePtr, boost::hash<DatabaseTable>>;
    using ClusterContainer = std::unordered_map<DatabaseTable, ClusterPtr, boost::hash<DatabaseTable>>;

    TableContainer buildCatalog(const NodePtr & node, const Block & bock);
    void mergeCatalog(const NodePtr & node, TableContainer snapshot);

private:
    using TableContainerByNode = std::unordered_map<String, TablePtr>;

    mutable std::shared_mutex catalog_rwlock;
    /// (database, table) -> (node_identity -> TablePtr))
    std::unordered_map<DatabaseTable, TableContainerByNode, boost::hash<DatabaseTable>> indexed_by_name;
    /// node_identity -> ((database, table) -> TablePtr))
    std::unordered_map<String, TableContainer> indexed_by_node;
    std::unordered_map<String, NodePtr> cluster_nodes;

    mutable std::shared_mutex storage_rwlock;
    std::unordered_map<UUID, TablePtr> indexed_by_id;
    /// Virtual storages
    StorageContainer storages;

    mutable std::shared_mutex table_cluster_rwlock;
    ClusterContainer table_clusters;
};
}
