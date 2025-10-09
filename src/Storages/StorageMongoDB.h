#pragma once

#include "config.h"

#if USE_MONGODB
#include <Common/RemoteHostFilter.h>

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

namespace DB
{

class IAST;
class ASTLiteral;
class ASTFunction;

/// As per MongoDB CXX driver documentation:
/// You must create a mongocxx::instance object before you use the C++ driver,
/// and this object must remain alive for as long as any other MongoDB objects are in scope.
///
/// mongocxx::instance must not be created more than once, therefore we use a singleton.
class MongoDBInstanceHolder final : public boost::noncopyable
{
public:
    MongoDBInstanceHolder(MongoDBInstanceHolder const &) = delete;
    void operator=(MongoDBInstanceHolder const &) = delete;

    static MongoDBInstanceHolder & instance()
    {
        static MongoDBInstanceHolder instance;
        return instance;
    }

private:
    MongoDBInstanceHolder() = default;
    mongocxx::instance inst;
};

struct MongoDBConfiguration
{
    std::unique_ptr<mongocxx::uri> uri;
    String collection;
    std::unordered_set<String> oid_fields = {"_id"};

    void checkHosts(const ContextPtr & context) const;

    bool isOidColumn(const std::string & name) const { return oid_fields.contains(name); }
};

/** Implements storage in the MongoDB database.
 *  Use ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
 *               MongoDB(uri, collection[, oid columns]);
 *  One stream only.
 */
class StorageMongoDB final : public IStorage
{
public:
    static MongoDBConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageMongoDB(
        const StorageID & table_id_,
        MongoDBConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "MongoDB"; }
    bool isRemote() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// proton: starts
    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override;
    /// proton: ends

private:
    MongoDBInstanceHolder & instance_holder = MongoDBInstanceHolder::instance();

    /// proton: starts. These functions are rewritten without Analyzer
    const ColumnDescription * getColumn(const ASTPtr & node, const StorageSnapshotPtr & storage_snapshot);

    std::optional<bsoncxx::document::value>
    visitWhereFunction(const ContextPtr & context, const ASTFunction * func, const StorageSnapshotPtr & storage_snapshot);

    std::optional<bsoncxx::document::value> visitWhereFunctionArguments(
        const ColumnDescription * column_node, const ASTLiteral * const_node, const ASTFunction * func, bool invert_comparison);

    std::optional<bsoncxx::document::value>
    visitWhereNode(const ContextPtr & context, const ASTPtr & where_node, const StorageSnapshotPtr & storage_snapshot);
    /// proton: ends

    bsoncxx::document::value buildMongoDBQuery(
        const ContextPtr & context,
        mongocxx::options::find & options,
        const StorageSnapshotPtr & storage_snapshot,  /// proton: updates
        const SelectQueryInfo & query_info,
        const Block & sample_block);

    const MongoDBConfiguration configuration;
    LoggerPtr log;
};

}
#endif
