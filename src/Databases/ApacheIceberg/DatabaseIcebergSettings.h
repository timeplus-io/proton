#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct DatabaseApacheIcebergSettingsImpl;
class SettingsChanges;

/// List of available types supported in DatabaseIcebergSettings object
#define DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, IcebergCatalogType)

DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseApacheIcebergSettings, DECLARE_SETTING_TRAIT)

struct DatabaseApacheIcebergSettings
{
    DatabaseApacheIcebergSettings();
    DatabaseApacheIcebergSettings(const DatabaseApacheIcebergSettings & settings);
    DatabaseApacheIcebergSettings(DatabaseApacheIcebergSettings && settings) noexcept;
    ~DatabaseApacheIcebergSettings();

    DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseApacheIcebergSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(const ASTStorage & storage_def);

    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<DatabaseApacheIcebergSettingsImpl> impl;
};
}
