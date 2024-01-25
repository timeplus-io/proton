#include <Core/QueryProcessingStage.h>
#include <Client/Connection.h>
#include <Client/ConnectionParameters.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/StorageFactory.h>
#include "Storages/ExternalTable/ClickHouse/ClickHouse.h"

namespace DB
{

StorageExternalTable::StorageExternalTable(
        const StorageID & table_id_,
        std::unique_ptr<ExternalTableSettings>  settings,
        ContextPtr context_)
: IStorage(table_id_)
, WithContext(context_->getGlobalContext())
{
    auto type = settings->type.value;
    if (type == "clickhouse")
    {
        auto ctx = getContext();
        external_table = std::make_unique<ExternalTable::ClickHouse>(std::move(settings), ctx);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown external table type: {}", type);
}

SinkToStoragePtr StorageExternalTable::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_)
{
    return external_table->write(query, metadata_snapshot, context_);
}

void registerStorageExternalTable(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        if (args.storage_def->settings)
        {
            auto settings = std::make_unique<ExternalTableSettings>();
            settings->loadFromQuery(*args.storage_def);

            return StorageExternalTable::create(args.table_id, std::move(settings), args.getContext());
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External table requires correct settings setup");
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
