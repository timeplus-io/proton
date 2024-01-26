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
    explicit ClickHouse(ExternalTableSettingsPtr settings, ContextPtr & context_);

    void startup() override;
    void shutdown() override {}

    ColumnsDescription getTableStructure() override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

private:
    ConnectionParameters connection_params;
    String table;

    ContextPtr & context;
    Poco::Logger * logger;
};

}

}
