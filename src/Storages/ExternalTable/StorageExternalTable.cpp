#include <Storages/ExternalTable/StorageExternalTable.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <base/sleep.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

namespace DB
{

StorageExternalTable::StorageExternalTable(
    const StorageID & table_id,
    const StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalTableSettings> settings_,
    bool attach_,
    ContextPtr context_)
    : IStorage(table_id)
    , WithContext(context_->getGlobalContext())
    , settings(std::move(settings_))
    , attach{attach_}
    , background_jobs(std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1))
{
    setInMemoryMetadata(storage_metadata);
}

void StorageExternalTable::startup()
{
    updateTableSchema(attach);
    LOG_INFO(getLogger("StorageExternalTable"), "External table `{}` started", getStorageID().getFullTableName());
}

void StorageExternalTable::updateTableSchema(bool retry_in_background)
{
    /// Two situations:
    /// * Create a new table. In this case, we want it fails the create query if it fails to fetch the columns description. So that users know that there is something with the connection and they can try again once the issue is resolved.
    /// * Attach a table (Proton restarts). In this case, even it fails to fetch the columns description, we want to make sure that:
    ///   - it does not terminate Proton, otherwise Proton will never start again
    ///   - it does not block Proton from starting, otherwise Proton will get stuck
    ///   So, we let it keep retrying in the background, and hoping it will eventually succeeded (until the user drops the table).
    ///
    /// TODO we could use cache to save the table structure, so that when Proton restarts it could read from the cache directly.
    try
    {
        updateTableSchema();
    }
    catch (const Exception & e)
    {
        if (!retry_in_background)
            e.rethrow();

        LOG_ERROR(
            getLogger("ExternalTable"),
            "Failed to fetch table structure for {}, error: {}. Will keep retrying in background",
            getStorageID().getFullTableName(),
            e.what());

        background_jobs->scheduleOrThrowOnError([this]() {
            setThreadName("ExtTblFetcher");
            while (!is_dropped)
            {
                try
                {
                    sleepForSeconds(5);
                    updateTableSchema();
                    return;
                }
                catch (...)
                {
                    tryLogCurrentException(
                        getLogger("ExternalTable"),
                        "Failed to fetch table structure for " + getStorageID().getFullTableName() + ", will retry");
                }
            }
        });
    }
}

void StorageExternalTable::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    setThreadName("ExtTblReader");
    readImpl(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

void StorageExternalTable::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    IStorage::read(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageExternalTable::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    setThreadName("ExtTblWriter");
    return writeImpl(query, metadata_snapshot, local_context);
}

void StorageExternalTable::alter(const AlterCommands & params, ContextPtr context_, AlterLockHolder &)
{
    if (unlikely(params.empty()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter command can not be empty.");

    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context_);

    /// Propose stream metadata change and alter command. External table metadata will be updated by Metadata Updater.
    auto db = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    db->alterTable(context_, table_id, new_metadata, params.front().typeString(), params.astCommands(table_id));
}

void StorageExternalTable::updateTableSchema()
{
    ColumnsDescription desc;
    getTableSchema(getContext(), desc);

    if (!desc.empty())
    {
        auto metadata = getInMemoryMetadata();
        metadata.setColumns(std::move(desc));
        setInMemoryMetadata(metadata);
    }
    else if (getInMemoryMetadataPtr()->getColumns().empty())
    {
        LOG_WARNING(
            getLogger("StorageExternalTable"),
            "External table `{}` has an empty schema (none fetched from the remote system nor declared at create "
            "time); materialized views writing to it will fail with no matching columns",
            getStorageID().getFullTableName());
    }

    /// getTableSchema() throws on failure, so reaching here means the schema is determined (possibly empty);
    /// mark ready so MV checkDependencies() proceeds instead of waiting forever on a legitimately empty schema.
    ready = true;
}

void registerStorageExternalTable(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args) { return ExternalTableFactory::instance().getExternalTable(args); };

    factory.registerStorage(
        "ExternalTable",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true, // for partition by
            .supports_schema_inference = true,
        });
}
}
