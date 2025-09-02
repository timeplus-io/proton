#pragma once

#include "config.h"

#if USE_AVRO
#include <Databases/ApacheIceberg/DatabaseIcebergSettings.h>
#include <Storages/Iceberg/ICatalog.h>
#include <Databases/DatabasesCommon.h>
#include <Poco/Net/HTTPBasicCredentials.h>

namespace DB
{

class DatabaseApacheIceberg final : public IDatabase, WithContext
{
public:
    explicit DatabaseApacheIceberg(
        const std::string & database_name_,
        const std::string & url_,
        const DatabaseApacheIcebergSettings & settings_,
        ASTPtr database_engine_definition_,
        bool attach);

    String getEngineName() const override { return "Iceberg"; }
    bool configureTableEngine(ASTCreateQuery &) const override;

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    void shutdown() override { }

    ASTPtr getCreateDatabaseQuery() const override;

    void createTable(ContextPtr /*context*/, const String & /*name*/, const StoragePtr & /*table*/, const ASTPtr & /*query*/) override;

    void dropTable(ContextPtr /*context*/, const String & /*name*/, bool /*sync = false*/, const ASTPtr & /*query*/) override;

    /// FIXME this should not be a public function, just a temporary trick.
    Apache::Iceberg::CatalogPtr getCatalog() const;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

    ASTPtr generateCreateTableQuery(const String & table_name, const StoragePtr & storage, bool throw_on_error) const;

private:
    void validateSettings();
    void parseStorageCredentials();
    void initCatalog() const;
    /*std::shared_ptr<StorageObjectStorage::Configuration> getConfiguration(DatabaseIcebergStorageType type) const;*/
    std::string getStorageEndpointForTable(const Apache::Iceberg::TableMetadata & table_metadata) const;

    /// Iceberg Catalog url.
    const std::string url;
    /// SETTINGS from CREATE query.
    const DatabaseApacheIcebergSettings settings;
    /// Database engine definition taken from initial CREATE DATABASE query.
    const ASTPtr database_engine_definition;

    mutable std::mutex catalog_impl_mutex;
    mutable Apache::Iceberg::CatalogPtr catalog_impl;

    std::shared_ptr<Apache::Iceberg::IStorageCredentials> storage_credentials;

    LoggerPtr log;
};

}
#endif
