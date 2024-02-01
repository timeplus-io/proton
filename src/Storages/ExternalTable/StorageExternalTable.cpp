#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouse.h>
#include "Storages/ExternalTable/ExternalTableFactory.h"

namespace DB
{

StorageExternalTable::StorageExternalTable(
        std::unique_ptr<ExternalTableSettings>  settings,
        const StorageFactory::Arguments & args)
: IStorage(args.table_id)
, WithContext(args.getContext()->getGlobalContext())
{
    external_table = ExternalTableFactory::instance().getExternalTable(args.table_id.getTableName(), std::move(settings));

    setStorageMetadata(args);
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
