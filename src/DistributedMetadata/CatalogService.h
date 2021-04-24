#pragma once

#include "MetadataService.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/Cluster.h>
#include <Processors/QueryPipeline.h>

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
        UInt64 total_rows = 0;
        UInt64 total_bytes = 0;
        UInt8 is_temporary = 0;
        Int32 shard = 0;

        Table(const String & node_identity_, const String & host_) : node_identity(node_identity_), host(host_) { }
    };

    using TablePtr = std::shared_ptr<Table>;
    using TablePtrs = std::vector<TablePtr>;

    struct Node
    {
        /// Node identity
        String identity;

        /// `host` is network reachable like hostname, FQDN or IP
        String host;

        Int32 http_port = 8123;
        Int32 tcp_port = 9000;

        Node(const String & identity_, const std::unordered_map<String, String> & headers)
        {
            identity = identity_;

            auto iter = headers.find("_host");
            if (iter != headers.end())
            {
                host = iter->second;
            }

            iter = headers.find("_http_port");
            if (iter != headers.end())
            {
                http_port = std::stoi(iter->second);
            }

            iter = headers.find("_tcp_port");
            if (iter != headers.end())
            {
                tcp_port = std::stoi(iter->second);
            }
        }

        bool isValid() const { return !identity.empty() && !host.empty() && http_port > 0 && tcp_port > 0; }

        String string() const { return identity + "," + host + "," + std::to_string(http_port) + "," + std::to_string(tcp_port); }
    };
    using NodePtr = std::shared_ptr<Node>;

public:
    static CatalogService & instance(const ContextPtr & context_);

    explicit CatalogService(const ContextPtr & context_);
    virtual ~CatalogService() override = default;

    /// `broadcast` broadcasts the table catalog metadata on this node
    /// to all nodes with CatalogService role in the cluster via DWAL
    void broadcast();

    StoragePtr createVirtualTableStorage(const String & query, const String & database, const String & table);

    std::pair<TablePtr, StoragePtr> findTableStorageById(const UUID & uuid) const;
    std::pair<TablePtr, StoragePtr> findTableStorageByName(const String & database, const String & table) const;

    TablePtrs findTableByName(const String & database, const String & table) const;
    TablePtrs findTableByNode(const String & node_identity) const;
    TablePtrs findTableByDB(const String & database) const;

    bool tableExists(const String & database, const String & table) const;

    TablePtrs tables() const;
    std::vector<String> databases() const;

    ClusterPtr tableCluster(const String & database, const String & table, Int32 replication_factor, Int32 shards);

private:
    bool setTableStorageByName(const String & database, const String & table, const StoragePtr & storage);

    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "catalog"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(1000, 200); }

private:
    void doBroadcast();
    void processQuery(BlockInputStreamPtr & in);
    void processQueryWithProcessors(QueryPipeline & pipeline);
    void append(Block && block);

private:
    using NodeShard = std::pair<String, Int32>;
    using TableShard = std::pair<String, Int32>;
    using DatabaseTable = std::pair<String, String>;
    using DatabaseTableShard = std::pair<String, TableShard>;

    /// In the cluster, (database, table, shard) is unique
    using TableContainerPerNode = std::unordered_map<DatabaseTableShard, TablePtr, boost::hash<DatabaseTableShard>>;

    TableContainerPerNode buildCatalog(const NodePtr & node, const Block & bock);
    void mergeCatalog(const NodePtr & node, TableContainerPerNode snapshot);

private:
    using TableContainerByNodeShard = std::unordered_map<NodeShard, TablePtr, boost::hash<NodeShard>>;

    mutable std::shared_mutex catalog_rwlock;

    /// (database, table) -> ((node_identity, shard) -> TablePtr))
    std::unordered_map<DatabaseTable, TableContainerByNodeShard, boost::hash<DatabaseTable>> indexed_by_name;

    /// node_identity -> ((database, table, shard) -> TablePtr))
    std::unordered_map<String, TableContainerPerNode> indexed_by_node;
    std::unordered_map<String, NodePtr> table_nodes;

    mutable std::shared_mutex storage_rwlock;
    std::unordered_map<UUID, TablePtr> indexed_by_id;
    std::unordered_map<DatabaseTable, StoragePtr, boost::hash<DatabaseTable>> storages;

    mutable std::shared_mutex table_cluster_rwlock;
    std::unordered_map<DatabaseTable, ClusterPtr, boost::hash<DatabaseTable>> table_clusters;
};
}
