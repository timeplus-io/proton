#include <Databases/ApacheIceberg/DatabaseIcebergSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

#define DATABASE_ICEBERG_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(IcebergCatalogType, catalog_type, IcebergCatalogType::REST, "Catalog type", 0) \
    DECLARE(String, catalog_credential, "", "Credentials to be used for accessing the catalog service, format: 'username:password'", 0) \
    DECLARE(Bool, use_environment_credentials, true, "Use credentials from environment to access catalog and storage, where it's applicable", 0) \
    DECLARE(String, auth_scope, "PRINCIPAL_ROLE:ALL", "Authorization scope for client credentials or token exchange", 0) \
    DECLARE(String, oauth_server_uri, "", "OAuth server uri", 0) \
    DECLARE(String, warehouse, "", "Warehouse name inside the catalog", 0) \
    DECLARE(String, auth_header, "", "Authorization header of format 'Authorization: <scheme> <auth_info>'", 0) \
    DECLARE(String, rest_catalog_uri, "", "URI identifying the REST catalog server", 0) \
    DECLARE( \
        Bool, rest_catalog_sigv4_enabled, false, "Set to true to sign requests to the REST catalog server using AWS SigV4 protocol", 0) \
    DECLARE(String, rest_catalog_signing_region, "", "The region to use when SigV4 signing a request", 0) \
    DECLARE(String, rest_catalog_signing_name, "", "The service signing name to use when SigV4 signing a request", 0) \
    DECLARE(String, storage_endpoint, "", "Storage endpoint", 0) \
    DECLARE(Bool, vended_credentials, false, "Use vended credentials (storage credentials) from catalog", 0) \
    DECLARE(String, storage_credential, "", "Credentials to be used for accessing the storage, if vended credentials are not used, format: 'username:password'", 0) \
    DECLARE(String, credential, "", "Credentials to be used for accessing both the catalog service and the storage, if vended credentials are not used, format: 'username:password'", 0)

#define LIST_OF_DATABASE_ICEBERG_SETTINGS(M, ALIAS) DATABASE_ICEBERG_RELATED_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(DatabaseApacheIcebergSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseApacheIcebergSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)

struct DatabaseApacheIcebergSettingsImpl : public BaseSettings<DatabaseApacheIcebergSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    DatabaseApacheIcebergSettings##TYPE NAME = &DatabaseApacheIcebergSettingsImpl ::NAME;

namespace DatabaseApacheIcebergSetting
{
LIST_OF_DATABASE_ICEBERG_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

DatabaseApacheIcebergSettings::DatabaseApacheIcebergSettings() : impl(std::make_unique<DatabaseApacheIcebergSettingsImpl>())
{
}

DatabaseApacheIcebergSettings::DatabaseApacheIcebergSettings(const DatabaseApacheIcebergSettings & settings)
    : impl(std::make_unique<DatabaseApacheIcebergSettingsImpl>(*settings.impl))
{
}

DatabaseApacheIcebergSettings::DatabaseApacheIcebergSettings(DatabaseApacheIcebergSettings && settings) noexcept
    : impl(std::make_unique<DatabaseApacheIcebergSettingsImpl>(std::move(*settings.impl)))
{
}

DatabaseApacheIcebergSettings::~DatabaseApacheIcebergSettings() = default;

DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseApacheIcebergSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void DatabaseApacheIcebergSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void DatabaseApacheIcebergSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (storage_def.settings != nullptr)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for database engine " + storage_def.engine->name);
            throw;
        }
    }
}

}
