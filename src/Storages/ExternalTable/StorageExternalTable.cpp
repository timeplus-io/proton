#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/ClickHouse.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/ExternalTable/StorageExternalTable.h>

namespace DB
{

StorageExternalTable::StorageExternalTable(
    const StorageID & table_id,
    std::unique_ptr<ExternalTableSettings>  settings,
    bool is_attach,
    ContextPtr context_)
: IStorage(table_id)
, WithContext(context_)
{
    external_table = ExternalTableFactory::instance().getExternalTable(table_id.getTableName(), std::move(settings));

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
        fetchColumnsDescription();
    }
    catch (const Exception & e)
    {
        if (!is_attach)
            e.rethrow();

        LOG_ERROR(&Poco::Logger::get("ExternalTable"),
                  "Failed to fetch table structure for {}, error: {}. Will keep retrying in background",
                  getStorageID().getFullTableName(),
                  e.what());
        background_jobs.scheduleOrThrowOnError([this](){
            while (!is_dropped)
            {
                try
                {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    fetchColumnsDescription();
                    return;
                }
                catch (const Exception &) { }
            }
        });
    }
}

Pipe StorageExternalTable::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams)
{
    return external_table->read(column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageExternalTable::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_)
{
    return external_table->write(query, metadata_snapshot, context_);
}

void StorageExternalTable::fetchColumnsDescription()
{
        auto desc = external_table->getTableStructure();
        auto metadata = getInMemoryMetadata();
        metadata.setColumns(std::move(desc));
        setInMemoryMetadata(metadata);
}

void registerStorageExternalTable(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External table requires correct settings setup");

        auto settings = std::make_unique<ExternalTableSettings>();
        settings->loadFromQuery(*args.storage_def);

        return StorageExternalTable::create(
                args.table_id,
                std::move(settings),
                args.attach,
                args.getContext()->getGlobalContext());
    };

    factory.registerStorage(
        "ExternalTable",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_schema_inference = true,
        });
}

}
