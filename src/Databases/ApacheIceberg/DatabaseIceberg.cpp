#include <Databases/ApacheIceberg/DatabaseIceberg.h>

#if USE_AVRO
#include <Core/Settings.h>
#include <Common/LRUCache.h>

#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Storages/Iceberg/RestCatalog.h>

#include <Storages/ConstraintsDescription.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/StorageNull.h>

#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Formats/FormatFactory.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace DatabaseApacheIcebergSetting
{
extern const DatabaseApacheIcebergSettingsIcebergCatalogType catalog_type;
extern const DatabaseApacheIcebergSettingsString warehouse;
extern const DatabaseApacheIcebergSettingsString catalog_credential;
extern const DatabaseApacheIcebergSettingsString auth_header;
extern const DatabaseApacheIcebergSettingsString auth_scope;
extern const DatabaseApacheIcebergSettingsString storage_endpoint;
extern const DatabaseApacheIcebergSettingsString oauth_server_uri;
extern const DatabaseApacheIcebergSettingsBool vended_credentials;
extern const DatabaseApacheIcebergSettingsString rest_catalog_uri;
extern const DatabaseApacheIcebergSettingsBool rest_catalog_sigv4_enabled;
extern const DatabaseApacheIcebergSettingsString rest_catalog_signing_region;
extern const DatabaseApacheIcebergSettingsString rest_catalog_signing_name;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INVALID_SETTING_VALUE;
extern const int SUPPORT_IS_DISABLED;
extern const int UNKNOWN_DATABASE;
extern const int UNSUPPORTED;
}

namespace
{
/// Parse a string, containing at least one dot, into a two substrings:
/// A.B.C.D.E -> A.B.C.D and E, where
/// `A.B.C.D` is a table "namespace".
/// `E` is a table name.
std::pair<std::string, std::string> parseTableName(const std::string & name)
{
    auto pos = name.rfind('.');
    if (pos == std::string::npos)
        return {"", name};

    auto table_name = name.substr(pos + 1);
    auto namespace_name = name.substr(0, name.size() - table_name.size() - 1);
    return {std::move(namespace_name), std::move(table_name)};
}

auto & tableCache()
{
    static LRUCache<std::string, IStorage> table_cache{/*max_size_=*/1000};
    return table_cache;
}
}

DatabaseApacheIceberg::DatabaseApacheIceberg(
    const std::string & database_name_,
    const std::string & url_,
    const DatabaseApacheIcebergSettings & settings_,
    ASTPtr database_engine_definition_)
    : IDatabase(database_name_)
    , url(url_)
    , settings(settings_)
    , database_engine_definition(database_engine_definition_)
    , log(&Poco::Logger::get(fmt::format("DatabaseIceberg({})", database_name)))
{
    validateSettings();
    initCatalog();
    if (!getCatalog()->existsNamespace(getDatabaseName()))
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_DATABASE, "Namespace {} does not exist in the catalog", getDatabaseName());
}

void DatabaseApacheIceberg::initCatalog()
{
    switch (settings[DatabaseApacheIcebergSetting::catalog_type].value)
    {
        case DB::IcebergCatalogType::REST:
        {
            catalog_impl = std::make_shared<Apache::Iceberg::RestCatalog>(
                settings[DatabaseApacheIcebergSetting::warehouse].value,
                url,
                Apache::Iceberg::RestCatalog::Options{
                    .catalog_credential = settings[DatabaseApacheIcebergSetting::catalog_credential].value,
                    .auth_scope = settings[DatabaseApacheIcebergSetting::auth_scope].value,
                    .auth_header = settings[DatabaseApacheIcebergSetting::auth_header],
                    .oauth_server_uri = settings[DatabaseApacheIcebergSetting::oauth_server_uri].value,
                    .enable_sigv4 = settings[DatabaseApacheIcebergSetting::rest_catalog_sigv4_enabled].value,
                    .signing_region = settings[DatabaseApacheIcebergSetting::rest_catalog_signing_region].value,
                    .signing_name = settings[DatabaseApacheIcebergSetting::rest_catalog_signing_name].value,
                },
                Context::getGlobalContextInstance());

            break;
        }
    }
}

void DatabaseApacheIceberg::validateSettings()
{
    if (settings[DatabaseApacheIcebergSetting::warehouse].value.empty())
    {
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "`warehouse` setting cannot be empty. "
            "Please specify 'SETTINGS warehouse=<warehouse_name>' in the CREATE DATABASE query");
    }
}

Apache::Iceberg::CatalogPtr DatabaseApacheIceberg::getCatalog() const
{
    return catalog_impl;
}

std::string DatabaseApacheIceberg::getStorageEndpointForTable(const Apache::Iceberg::TableMetadata & table_metadata) const
{
    auto endpoint_from_settings = settings[DatabaseApacheIcebergSetting::storage_endpoint].value;
    if (!endpoint_from_settings.empty())
    {
        return std::filesystem::path(endpoint_from_settings) / table_metadata.getLocation(/* path_only */ true) / "";
    }
    else
    {
        return std::filesystem::path(table_metadata.getLocation(/* path_only */ false)) / "";
    }
}

bool DatabaseApacheIceberg::empty() const
{
    return getCatalog()->empty(getDatabaseName());
}

bool DatabaseApacheIceberg::isTableExist(const String & name, ContextPtr /* context_ */) const
{
    const auto [namespace_name, table_name] = parseTableName(name);
    return getCatalog()->existsTable(namespace_name.empty() ? getDatabaseName() : fmt::format("{}.{}", getDatabaseName(), namespace_name), table_name);
}

StoragePtr DatabaseApacheIceberg::tryGetTable(const String & name [[maybe_unused]], ContextPtr context_ [[maybe_unused]]) const
{
    auto catalog = getCatalog();
    auto table_metadata = Apache::Iceberg::TableMetadata().withLocation().withSchema();

    const bool with_vended_credentials = settings[DatabaseApacheIcebergSetting::vended_credentials].value;
    if (with_vended_credentials)
        table_metadata = table_metadata.withStorageCredentials();

    auto cache_key = fmt::format("{}.{}", getDatabaseName(), name);

    if (!catalog->tryGetTableMetadata(getDatabaseName(), name, table_metadata))
    {
        tableCache().remove(cache_key);
        return nullptr;
    }

    auto [table, _] = tableCache().getOrSet(cache_key, [&]() {
        /// Replace Iceberg Catalog endpoint with storage path endpoint of requested table.
        auto table_endpoint = getStorageEndpointForTable(table_metadata);
        LOG_TEST(log, "Using table endpoint: {}", table_endpoint);

        auto stream_settings = std::make_shared<ASTSetQuery>();
        stream_settings->changes.setSetting("type", "iceberg");
        stream_settings->changes.setSetting("iceberg_storage_endpoint", table_endpoint);

        /// We either fetch storage credentials from catalog
        /// or get storage credentials from database engine arguments
        /// in CREATE query (e.g. in `args`).
        /// Vended credentials can be disabled in catalog itself,
        /// so we have a separate setting to know whether we should even try to fetch them.
        if (with_vended_credentials)
        {
            auto storage_credentials = table_metadata.getStorageCredentials();
            if (storage_credentials)
                storage_credentials->addCredentialsToSettings(*stream_settings);
        }

        ASTStorage stream_storage;
        stream_storage.set(stream_storage.settings, stream_settings);

        return StorageExternalStream::create(
            /*engine_args=*/ASTs{},
            /*table_id_=*/StorageID{getDatabaseName(), name},
            context_,
            ColumnsDescription(table_metadata.getSchema()),
            /*comment=*/"",
            &stream_storage,
            /*attach=*/false);
    });

    return table;
}

DatabaseTablesIteratorPtr
DatabaseApacheIceberg::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name) const
{
    Tables tables;
    auto catalog = getCatalog();
    const auto iceberg_tables = catalog->getTables(getDatabaseName());

    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        auto storage = tryGetTable(table_name, context_);
        [[maybe_unused]] bool inserted = tables.emplace(table_name, storage).second;
        chassert(inserted);
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(tables), getDatabaseName());
}

ASTPtr DatabaseApacheIceberg::getCreateDatabaseQuery() const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_definition);
    return create_query;
}

namespace
{

void validateCreate(const ASTPtr & query)
{
    auto create = std::dynamic_pointer_cast<ASTCreateQuery>(query);
    if (create->isView())
        throw Exception(ErrorCodes::UNSUPPORTED, "Views are not supported in Iceberg database");

    if (create->is_dictionary)
        throw Exception(ErrorCodes::UNSUPPORTED, "Dictionary is not supported in Iceberg database");

    if (create->is_external)
    {
        if (create->storage == nullptr || create->storage->settings == nullptr
            || create->storage->settings->changes.tryGet("type") == nullptr
            || create->storage->settings->changes.tryGet("type")->get<String>() != "iceberg")
            throw Exception(ErrorCodes::UNSUPPORTED, "External streams are not supported in Iceberg database");
    }

    if (create->is_random)
        throw Exception(ErrorCodes::UNSUPPORTED, "Random stream is not supported in Iceberg database");

    if (create->is_virtual)
        throw Exception(ErrorCodes::UNSUPPORTED, "Virtual stream is not supported in Iceberg database");
}

}

void DatabaseApacheIceberg::createTable(ContextPtr /*context*/, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    validateCreate(query);

    auto endpoint = settings[DatabaseApacheIcebergSetting::storage_endpoint].value;
    auto endpoint_uri = Poco::URI(endpoint);
    /// The table location requires a S3 URI (starts with 's3://')
    if (endpoint_uri.getScheme().starts_with("http"))
    {
        std::vector<String> parts;
        /// https://Bucket.s3.Region.amazonaws.com
        parts.reserve(5);
        splitInto<'.'>(parts, endpoint_uri.getHost());
        endpoint = "s3://" + parts[0] + "/";
    }

    getCatalog()->createTable(
        getDatabaseName(),
        name,
        endpoint + (endpoint.ends_with('/') ? "" : "/") + name,
        table->getInMemoryMetadata().getColumns().getAllPhysical(),
        std::nullopt,
        std::nullopt,
        /*stage_create=*/false,
        {});
}

void DatabaseApacheIceberg::dropTable( ContextPtr /*context*/, const String & name, bool /*no_delay*/)
{
    getCatalog()->deleteTable(getDatabaseName(), name);
    tableCache().remove(fmt::format("{}.{}", getDatabaseName(), name));
}

ASTPtr DatabaseApacheIceberg::getCreateTableQueryImpl(const String & name, ContextPtr /* context_ */, bool /* throw_on_error */) const
{
    auto table_metadata = Apache::Iceberg::TableMetadata().withLocation().withSchema();

    const auto [namespace_name, table_name] = parseTableName(name);
    getCatalog()->getTableMetadata(namespace_name.empty() ? getDatabaseName() : fmt::format("{}.{}", getDatabaseName(), namespace_name), table_name, table_metadata);

    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    create_table_query->setTable(name);
    create_table_query->setDatabase(getDatabaseName());

    for (const auto & column_type_and_name : table_metadata.getSchema())
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = makeASTDataType(column_type_and_name.type->getName());
        columns_expression_list->children.emplace_back(column_declaration);
    }

    return create_table_query;
}

bool DatabaseApacheIceberg::configureTableEngine(ASTCreateQuery & create) const
{
    if (create.storage == nullptr)
        create.set(create.storage, std::make_shared<ASTStorage>());

    create.is_external = true;
    if (create.storage->settings == nullptr)
        create.storage->set(create.storage->settings, std::make_shared<ASTSetQuery>());

    create.storage->settings->changes.setSetting("type", "iceberg");

    auto endpoint_from_settings = settings[DatabaseApacheIcebergSetting::storage_endpoint].value;
    create.storage->settings->changes.setSetting("iceberg_storage_endpoint", endpoint_from_settings);

    return true;
}

}

#endif
