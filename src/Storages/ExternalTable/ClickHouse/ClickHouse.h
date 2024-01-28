#pragma once

#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/ExternalTableImpl.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouse final : public IExternalTable
{
public:
    explicit ClickHouse(const String & name, ExternalTableSettingsPtr settings, ContextPtr & context [[maybe_unused]]);

    void startup() override;
    void shutdown() override {}

    ColumnsDescription getTableStructure() override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

private:
    ConnectionParameters connection_params;
    String table;

    Poco::Logger * logger;
};

}

}
