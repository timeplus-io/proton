#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class ASTColumns;
class TableRestRouterHandler : public RestRouterHandler
{
public:
    TableRestRouterHandler(ContextMutablePtr query_context_, const String & router_name) : RestRouterHandler(query_context_, router_name) { }
    ~TableRestRouterHandler() override { }

protected:
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
        String mode;
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
                && (lhs.name == rhs.name) && (lhs.uuid == rhs.uuid) && (lhs.engine == rhs.engine) && (lhs.mode == rhs.mode)
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

protected:
    static std::map<String, std::map<String, String>> update_schema;
    static std::map<String, String> granularity_func_mapping;

    static String getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity);
    static String getStringValueFrom(const Poco::JSON::Object::Ptr & payload, const String & key, const String & default_value);

    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;

    /// Return empty string if the request is invalid
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard, const String & uuid) const;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePatch(const Poco::JSON::Object::Ptr & payload) const override;

    void buildRetentionSettings(Poco::JSON::Object & resp_table, const String & database, const String & table) const;
    virtual void buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const;
    virtual void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_ast) const;

    virtual String getDefaultPartitionGranularity() const = 0;
    virtual String getDefaultOrderByGranularity() const = 0;
    virtual String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const = 0;
    virtual String getSettings(const Poco::JSON::Object::Ptr &) const { return ""; }
    virtual String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const = 0;
    virtual String subtype() const = 0;
    virtual bool isExternal() const { return false; }
};

}
