#pragma once

#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_EXTERNAL_TABLE_SETTINGS(M) \
  M(String, type, "", "External table type", 0) \
  /* ClickHouse settings */ \
  M(String, address, "", "The address of the ClickHouse server to connect", 0) \
  M(String, user, "default", "The user to be used to connect to the ClickHouse server", 0) \
  M(String, password, "", "The password to be used to connect to the ClickHouse server", 0) \
  M(Bool, secure, false, "Indicates if it uses secure connection", 0) \
  M(Bool, compression, true, "Indicates if compression should be enabled", 0) \
  M(String, database, "default", "The datababse to connect to", 0) \
  M(String, table, "", "The ClickHouse table to which the external table is mapped", 0)

DECLARE_SETTINGS_TRAITS(ExternalTableSettingsTraits, LIST_OF_EXTERNAL_TABLE_SETTINGS)


/// Settings for the ExternalTable engine.
/// Could be loaded from a CREATE EXTERNAL TABLE query (SETTINGS clause).
struct ExternalTableSettings : public BaseSettings<ExternalTableSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

using ExternalTableSettingsPtr = std::unique_ptr<ExternalTableSettings>;

}
