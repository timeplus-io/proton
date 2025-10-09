#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <Storages/PostgreSQL/PostgreSQLReplicationHandler.h>

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Parsers/ASTCreateQuery.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseAtomic.h>


namespace DB
{

struct MaterializedPostgreSQLSettings;
class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class DatabaseMaterializedPostgreSQL : public DatabaseAtomic
{

public:
    DatabaseMaterializedPostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        bool is_attach_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info,
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_);

    String getEngineName() const override { return "MaterializedPostgreSQL"; }

    String getMetadataPath() const override { return metadata_path; }

    DatabaseTablesIteratorPtr
    getTablesIterator(ContextPtr context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync, const ASTPtr & query) override;

    void drop(ContextPtr local_context) override;

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    void applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr query_context) override;

    void shutdown() override;

    String getPostgreSQLDatabaseName() const { return remote_database_name; }

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const override;

    /// proton: starts.
    ASTPtr generateCreateTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name, bool throw_on_error) const;
    /// proton: ends.
private:
    void tryStartSynchronization();
    void startSynchronization();

    ASTPtr createAlterSettingsQuery(const SettingChange & new_setting);

    String getFormattedTablesList(const String & except = {}) const;

    bool is_attach;
    String remote_database_name;
    postgres::ConnectionInfo connection_info;
    std::unique_ptr<MaterializedPostgreSQLSettings> settings;

    std::shared_ptr<PostgreSQLReplicationHandler> replication_handler;
    std::map<std::string, StoragePtr> materialized_tables;
    mutable std::mutex tables_mutex;
    mutable std::mutex handler_mutex;

    BackgroundSchedulePoolTaskHolder startup_task;
    bool shutdown_called = false;
};

}

#endif
