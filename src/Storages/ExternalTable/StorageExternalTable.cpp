#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouse.h>

namespace DB
{

StorageExternalTable::StorageExternalTable(
        std::unique_ptr<ExternalTableSettings>  settings,
        const StorageFactory::Arguments & args)
: IStorage(args.table_id)
, WithContext(args.getContext()->getGlobalContext())
{
    auto type = settings->type.value;
    if (type == "clickhouse")
    {
        auto ctx = getContext();
        external_table = std::make_unique<ExternalTable::ClickHouse>(args.table_id.getTableName(), std::move(settings), ctx);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown external table type: {}", type);

    setStorageMetadata(args);
}

SinkToStoragePtr StorageExternalTable::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_)
{
    return external_table->write(query, metadata_snapshot, context_);
}

void StorageExternalTable::setStorageMetadata(const StorageFactory::Arguments & args)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(external_table->getTableStructure());

    storage_metadata.setConstraints(args.constraints);
    storage_metadata.setComment(args.comment);
    setInMemoryMetadata(storage_metadata);
}

void registerStorageExternalTable(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External table requires correct settings setup");

        auto settings = std::make_unique<ExternalTableSettings>();
        settings->loadFromQuery(*args.storage_def);

        return StorageExternalTable::create(std::move(settings), args);
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
