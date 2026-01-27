#include <Storages/ExternalTable/ExternalTableFactory.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/NamedCollectionsHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int INVALID_SETTING_VALUE;
extern const int UNKNOWN_TYPE;
extern const int BAD_ARGUMENTS;
}

void registerS3ExternalTable(ExternalTableFactory & factory);
void registerClickHouseExternalTable(ExternalTableFactory & factory);
void registerMySQLExternalTable(ExternalTableFactory & factory);
void registerPostgreSQLExternalTable(ExternalTableFactory & factory);
void registerMongoDBExternalTable(ExternalTableFactory & factory);

ExternalTableFactory & ExternalTableFactory::instance()
{
    static DB::ExternalTableFactory ret;
    return ret;
}

void ExternalTableFactory::registerExternalTable(const String & type, Creator creator)
{
    if (creators.contains(type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExternalTableFactory: type {} is already registered", type);

    creators[type] = std::move(creator);
}

StoragePtr ExternalTableFactory::getExternalTable(const StorageFactory::Arguments & args) const
{
    if (args.storage_def->settings == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External table requires correct settings setup");

    auto context = Context::createCopy(args.getContext()->getGlobalContext());
    auto settings = context->getSettingsRef();

    auto external_table_settings = std::make_unique<ExternalTableSettings>();

    for (const auto & change : args.storage_def->settings->changes)
    {
        if (change.name.starts_with("s3_") && settings.has(change.name))
            context->setSetting(change.name, change.value);
        else if (external_table_settings->has(change.name))
            external_table_settings->set(change.name, change.value);
        else
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Unknow settings '{}'", change.name);
    }

    if (!external_table_settings->config_file.value.empty())
        external_table_settings->loadFromConfigFile(external_table_settings->config_file.value);

    if (!external_table_settings->named_collection.value.empty())
        updateSettingsByNamedCollection(*external_table_settings, context);

    auto type = external_table_settings->type.value;
    if (!creators.contains(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown external table type {}", type);

    /// Some external tables require the column information, thus we need to pass the metadata to them.
    StorageInMemoryMetadata storage_metadata;
    if (!args.comment.empty())
        storage_metadata.setComment(args.comment);

    if (!args.columns.empty())
    {
        storage_metadata.setColumns(args.columns);

        if (args.storage_def->partition_by != nullptr)
        {
            ASTPtr partition_by_ast{args.storage_def->partition_by->clone()};
            storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, args.columns, context);
        }
    }

    storage_metadata.setVersion(args.schema_version);

    return creators.at(type)(args.table_id, storage_metadata, args.query, std::move(external_table_settings), args.attach, context);
}

ExternalTableFactory::ExternalTableFactory()
{
    registerS3ExternalTable(*this);
    registerClickHouseExternalTable(*this);
    registerMySQLExternalTable(*this);
    registerPostgreSQLExternalTable(*this);
    registerMongoDBExternalTable(*this);
}

}
