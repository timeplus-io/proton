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
extern const DatabaseApacheIcebergSettingsString oauth_server_uri;
extern const DatabaseApacheIcebergSettingsString rest_catalog_uri;
extern const DatabaseApacheIcebergSettingsBool use_environment_credentials;
extern const DatabaseApacheIcebergSettingsBool rest_catalog_sigv4_enabled;
extern const DatabaseApacheIcebergSettingsString rest_catalog_signing_region;
extern const DatabaseApacheIcebergSettingsString rest_catalog_signing_name;
extern const DatabaseApacheIcebergSettingsString storage_endpoint;
extern const DatabaseApacheIcebergSettingsBool vended_credentials;
extern const DatabaseApacheIcebergSettingsString storage_credential;
extern const DatabaseApacheIcebergSettingsString credential;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CREATE_DATABASE;
extern const int INVALID_SETTING_VALUE;
extern const int SUPPORT_IS_DISABLED;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_STREAM;
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
    ASTPtr database_engine_definition_,
    bool attach)
    : IDatabase(database_name_)
    , url(url_)
    , settings(settings_)
    , database_engine_definition(database_engine_definition_)
    , log(getLogger(fmt::format("DatabaseIceberg({})", database_name)))
{
    validateSettings();
    parseStorageCredentials();

    try
    {
        initCatalog();
        if (!getCatalog()->existsNamespace(getDatabaseName()))
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_DATABASE, "Namespace {} does not exist in the catalog", getDatabaseName());
    }
    catch (...)
    {
        if (attach)
            tryLogCurrentException(log);
        else
            throw;
    }
}

void DatabaseApacheIceberg::initCatalog() const
{
    switch (settings[DatabaseApacheIcebergSetting::catalog_type].value)
    {
        case DB::IcebergCatalogType::REST:
        {
            auto credential = settings[DatabaseApacheIcebergSetting::catalog_credential].value;
            if (credential.empty())
                credential = settings[DatabaseApacheIcebergSetting::credential].value;

            catalog_impl = std::make_shared<Apache::Iceberg::RestCatalog>(
                settings[DatabaseApacheIcebergSetting::warehouse].value,
                url,
                Apache::Iceberg::RestCatalog::Options{
                    .catalog_credential = credential,
                    .auth_scope = settings[DatabaseApacheIcebergSetting::auth_scope].value,
                    .auth_header = settings[DatabaseApacheIcebergSetting::auth_header],
                    .oauth_server_uri = settings[DatabaseApacheIcebergSetting::oauth_server_uri].value,
                    .use_environment_credentials = settings[DatabaseApacheIcebergSetting::use_environment_credentials].value,
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

    auto validate_credential = [](const String & value, const String & name) {
        if (value.find(':') == std::string::npos)
            throw DB::Exception(
                DB::ErrorCodes::INVALID_SETTING_VALUE, "Unexpected format of {}: expected username and password separated by `:`", name);
    };

    if (auto credential = settings[DatabaseApacheIcebergSetting::catalog_credential].value; !credential.empty())
    {
        if (!settings[DatabaseApacheIcebergSetting::credential].value.empty())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE, "`credential` and `catalog_credential` settings should not be used at the same time");

        validate_credential(credential, "catalog_credential");
    }

    if (auto credential = settings[DatabaseApacheIcebergSetting::storage_credential].value; !credential.empty())
    {
        if (!settings[DatabaseApacheIcebergSetting::credential].value.empty())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE, "`credential` and `storage_credential` settings should not be used at the same time");

        if (settings[DatabaseApacheIcebergSetting::vended_credentials].value)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`vended_credentials` and `storage_credential` settings should not be used at the same time");

        validate_credential(credential, "storage_credential");
    }

    if (auto credential = settings[DatabaseApacheIcebergSetting::credential].value; !credential.empty())
        validate_credential(credential, "credential");
}

void DatabaseApacheIceberg::parseStorageCredentials()
{
    auto credential = settings[DatabaseApacheIcebergSetting::credential].value;
    if (credential.empty())
        credential = settings[DatabaseApacheIcebergSetting::storage_credential].value;
    if (credential.empty())
        return;

    auto pos = credential.find(':');
    chassert(pos != std::string::npos); /// validateSettings ensures this

    storage_credentials = std::make_shared<Apache::Iceberg::S3Credentials>(
        /*access_key_id_=*/credential.substr(0, pos), /*secret_access_key_=*/credential.substr(pos + 1), /*session_token_=*/"");
}

Apache::Iceberg::CatalogPtr DatabaseApacheIceberg::getCatalog() const
{
    std::lock_guard lock(catalog_impl_mutex);
    if (catalog_impl == nullptr)
        initCatalog();

    return catalog_impl;
}

std::string DatabaseApacheIceberg::getStorageEndpointForTable(const Apache::Iceberg::TableMetadata & table_metadata) const
{
    return std::filesystem::path(table_metadata.getLocation(/* path_only */ false)) / "";
}

bool DatabaseApacheIceberg::empty() const
{
    return getCatalog()->empty(getDatabaseName());
}

bool DatabaseApacheIceberg::isTableExist(const String & name, ContextPtr /* context_ */) const
{
    const auto [namespace_name, table_name] = parseTableName(name);
    return getCatalog()->existsTable(
        namespace_name.empty() ? getDatabaseName() : fmt::format("{}.{}", getDatabaseName(), namespace_name), table_name);
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
        /// or get storage credentials from database engine settings
        /// in CREATE query (e.g. in `SETTINGS`).
        /// Vended credentials can be disabled in catalog itself,
        /// so we have a separate setting to know whether we should even try to fetch them.
        if (with_vended_credentials)
        {
            if (auto credentials = table_metadata.getStorageCredentials(); credentials)
                credentials->addCredentialsToSettings(*stream_settings);
        }
        else
        {
            if (storage_credentials)
                storage_credentials->addCredentialsToSettings(*stream_settings);
        }

        stream_settings->changes.setSetting(
            "use_environment_credentials", settings[DatabaseApacheIcebergSetting::use_environment_credentials].value);

        ASTStorage stream_storage;
        stream_storage.set(stream_storage.settings, stream_settings);

        auto storage = StorageExternalStream::create(
            /*engine_args=*/ASTs{},
            /*table_id_=*/StorageID{getDatabaseName(), name},
            context_,
            ColumnsDescription(table_metadata.getSchema()),
            /*schema_version=*/1,
            /*comment=*/"",
            &stream_storage,
            /*attach=*/false,
            /*exec_script=*/std::nullopt);

        /// Set in-memory create query to ensure system.tables queries always use the in-memory metadata
        /// rather than falling back to ClickHouse's legacy code path, which could lead to inconsistencies
        ASTPtr origin_create_query = generateCreateTableQuery(name, storage, true);
        const auto & new_create_query = parseCreateQueryFromAST(origin_create_query, getDatabaseName(), name);
        storage->setInMemoryCreateQuery(new_create_query);

        return storage;
    });

    return table;
}

DatabaseTablesIteratorPtr
DatabaseApacheIceberg::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name) const
{
    Tables tables;

    /// Do not allow to throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some postgres error unless get_tables_ignore_error is false.
    try
    {
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
    }
    catch (...)
    {
        if (!context_->getSettingsRef().get_tables_ignore_error)
            throw;
        tryLogCurrentException(__PRETTY_FUNCTION__);
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

    if (create->isDictionary())
        throw Exception(ErrorCodes::UNSUPPORTED, "Dictionary is not supported in Iceberg database");

    if (create->isExternal())
    {
        if (create->storage == nullptr || create->storage->settings == nullptr
            || create->storage->settings->changes.tryGet("type") == nullptr
            || create->storage->settings->changes.tryGet("type")->get<String>() != "iceberg")
            throw Exception(ErrorCodes::UNSUPPORTED, "External streams are not supported in Iceberg database");
    }

    if (create->isRandomStream())
        throw Exception(ErrorCodes::UNSUPPORTED, "Random stream is not supported in Iceberg database");

    if (create->is_virtual)
        throw Exception(ErrorCodes::UNSUPPORTED, "Virtual stream is not supported in Iceberg database");

    if (create->isScheduledMaterializedView())
        throw Exception(ErrorCodes::UNSUPPORTED, "Scheduled Materialized View is not supported in Iceberg database");
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
        endpoint = "s3://" + parts[0] + endpoint_uri.getPath();
    }

    /// Set in-memory create query to ensure system.tables queries always use the in-memory metadata
    /// rather than falling back to ClickHouse's legacy code path, which could lead to inconsistencies
    ASTPtr origin_create_query = query->clone();
    const auto & new_create_query = parseCreateQueryFromAST(origin_create_query, getDatabaseName(), name);
    table->setInMemoryCreateQuery(new_create_query);

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

void DatabaseApacheIceberg::dropTable(ContextPtr /*context*/, const String & name, bool /*sync = false*/, const ASTPtr & /*query*/)
{
    getCatalog()->deleteTable(getDatabaseName(), name);
    tableCache().remove(fmt::format("{}.{}", getDatabaseName(), name));
}

ASTPtr DatabaseApacheIceberg::getCreateTableQueryImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    auto storage = tryGetTable(name, context_);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_STREAM, "Stream {}.{} doesn't exist", getDatabaseName(), name);
        return nullptr;
    }

    return generateCreateTableQuery(name, storage, throw_on_error);
}

ASTPtr DatabaseApacheIceberg::generateCreateTableQuery(const String & name, [[maybe_unused]] const StoragePtr & storage, bool /* throw_on_error */) const
{
    auto table_metadata = Apache::Iceberg::TableMetadata().withLocation().withSchema();

    const auto [namespace_name, table_name] = parseTableName(name);
    getCatalog()->getTableMetadata(
        namespace_name.empty() ? getDatabaseName() : fmt::format("{}.{}", getDatabaseName(), namespace_name), table_name, table_metadata);

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

    create.type = ASTCreateQuery::Type::ExternalStream;
    if (create.storage->settings == nullptr)
        create.storage->set(create.storage->settings, std::make_shared<ASTSetQuery>());

    create.storage->settings->changes.setSetting("type", "iceberg");

    auto endpoint_from_settings = settings[DatabaseApacheIcebergSetting::storage_endpoint].value;
    create.storage->settings->changes.setSetting("iceberg_storage_endpoint", endpoint_from_settings);

    if (storage_credentials)
        storage_credentials->addCredentialsToSettings(*create.storage->settings);

    create.storage->settings->changes.setSetting(
        "use_environment_credentials", settings[DatabaseApacheIcebergSetting::use_environment_credentials].value);

    return true;
}

void registerDatabaseIceberg(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args) {
        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        const ASTFunction * function_define = database_engine_define->engine;
        if (!function_define->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        ASTs & engine_args = function_define->arguments->children;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

        const auto url = engine_args[0]->as<ASTLiteral>()->value.safeGet<String>();

        DatabaseApacheIcebergSettings database_settings;
        if (database_engine_define->settings != nullptr)
            database_settings.loadFromQuery(*database_engine_define);

        try
        {
            return std::make_shared<DatabaseApacheIceberg>(
                args.database_name, url, database_settings, database_engine_define->clone(), args.create_query.attach);
        }
        catch (...)
        {
            const auto & exception_message = getCurrentExceptionMessage(true);
            throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE, "Cannot create Iceberg database, because {}", exception_message);
        }
    };
    factory.registerDatabase("Iceberg", create_fn, {.supports_arguments = true, .supports_settings = true});
}

}

#endif
