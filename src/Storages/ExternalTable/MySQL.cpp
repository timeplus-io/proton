#include <Storages/ExternalTable/MySQL.h>

#if USE_MYSQL
#    include <Bootstrap/Globals.h>
#    include <Databases/MySQL/FetchTablesColumnsList.h>
#    include <Interpreters/Context.h>
#    include <Storages/ColumnsDescription.h>
#    include <Storages/ExternalTable/ExternalTableFactory.h>
#    include <Storages/ExternalTable/ExternalTableSettings.h>
#    include <Storages/MySQL/MySQLHelpers.h>
#    include <Storages/MySQL/MySQLSettings.h>
#    include <Common/parseRemoteDescription.h>


namespace DB
{
namespace MySQLSetting
{
extern const MySQLSettingsUInt64 connection_pool_size;
extern const MySQLSettingsUInt64 connect_timeout;
extern const MySQLSettingsUInt64 read_write_timeout;
extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

void registerMySQLExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable(
        "mysql",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) -> StoragePtr {
            return std::make_shared<ExternalTable::MySQL>(table_id, storage_metadata, std::move(settings), attach, context);
        });
}

namespace ExternalTable
{
MySQL::MySQL(
    const StorageID & table_id,
    const StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalTableSettings> settings_,
    bool attach_,
    const ContextPtr & context_)
    : StorageExternalTable(table_id, storage_metadata, std::move(settings_), attach_, context_)
{
}

void MySQL::init()
{
    std::lock_guard lk{mutex};
    if (mysql)
        return;

    StorageMySQL::Configuration configuration;
    configuration.addresses = parseRemoteDescriptionForExternalDatabase(settings->address.value, 1, 3306);
    configuration.database = settings->database.value;
    configuration.username = settings->user.value;
    configuration.password = settings->password.value;

    auto context = getContext();
    const auto & context_settings = context->getSettingsRef();
    MySQLSettings mysql_settings;
    auto pool_size = settings->pooled_connections.changed ? settings->pooled_connections.value : DEFAULT_CONNECTION_POOL_SIZE;
    mysql_settings[MySQLSetting::connection_pool_size] = pool_size;
    /// MySQL settings have not exposed to external table settings.
    mysql_settings[MySQLSetting::connect_timeout] = context_settings.external_storage_connect_timeout_sec;
    mysql_settings[MySQLSetting::read_write_timeout] = context_settings.external_storage_rw_timeout_sec;
    mysql_settings[MySQLSetting::mysql_datatypes_support_level] = context_settings.mysql_datatypes_support_level;

    auto mysql_pool = createMySQLPoolWithFailover(configuration, mysql_settings);
    mysql = std::make_unique<StorageMySQL>(
        getStorageID(),
        std::move(mysql_pool),
        configuration.database,
        /*remote_table_name=*/settings->table.changed ? settings->table.value : getStorageID().getTableName(),
        /*replace_query_=*/settings->replace_query.value,
        /*on_duplicate_clause=*/settings->on_duplicate_clause.value,
        ColumnsDescription{},
        ConstraintsDescription{},
        /*comment=*/"",
        context,
        mysql_settings);
}
}
}

#else
namespace DB
{
class ExternalTableFactory;

void registerMySQLExternalTable(ExternalTableFactory &)
{
}
}

#endif
