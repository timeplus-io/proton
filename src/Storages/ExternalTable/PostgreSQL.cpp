#include <Storages/ExternalTable/PostgreSQL.h>

#include <Storages/ExternalTable/ExternalTableFactory.h>

#include "config.h"

#if USE_LIBPQXX
#    include <Core/PostgreSQL/PoolWithFailover.h>
#    include <Storages/StoragePostgreSQL.h>
#    include <Common/parseRemoteDescription.h>


namespace DB
{
void registerPostgreSQLExternalTable(ExternalTableFactory & factory) /// NOLINT
{
    factory.registerExternalTable(
        "postgresql",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) {
            return std::make_shared<ExternalTable::PostgreSQL>(table_id, storage_metadata, std::move(settings), attach, std::move(context));
        });
}

namespace ExternalTable
{
void PostgreSQL::init()
{
    std::lock_guard lk{mutex};
    if (storage)
        return;

    auto context = getContext();
    const auto & context_settings = context->getSettingsRef();

    auto remote_table_name = settings->table.changed ? settings->table.value : getStorageID().getTableName();

    postgres::PoolWithFailover::ReplicasConfigurationByPriority replicas_by_priority;
    StoragePostgreSQL::Configuration common_configuration;
    auto addresses = parseRemoteDescriptionForExternalDatabase(settings->address.value, 1, 5432);
    chassert(addresses.size() == 1);
    common_configuration.host = addresses[0].first;
    common_configuration.port = addresses[0].second;
    common_configuration.username = settings->user.value;
    common_configuration.password = settings->password.value;
    common_configuration.database = settings->database.value;
    common_configuration.schema = settings->schema.value;
    common_configuration.table = remote_table_name;
    replicas_by_priority[0].emplace_back(std::move(common_configuration));

    auto connection_pool_size = settings->pooled_connections.changed ? settings->pooled_connections : DEFAULT_CONNECTION_POOL_SIZE;
    auto pool = std::make_shared<postgres::PoolWithFailover>(
        replicas_by_priority,
        connection_pool_size,
        context_settings.postgresql_connection_pool_wait_timeout,
        context_settings.postgresql_connection_pool_retries,
        context_settings.postgresql_connection_pool_auto_close_connection,
        context_settings.postgresql_connection_attempt_timeout);

    storage = std::make_unique<StoragePostgreSQL>(
        getStorageID(),
        std::move(pool),
        remote_table_name,
        ColumnsDescription{},
        ConstraintsDescription{},
        /*comment=*/"",
        context);
}
}
}

#else

namespace DB
{
void registerPostgreSQLExternalTable(ExternalTableFactory &)
{
}
}

#endif
