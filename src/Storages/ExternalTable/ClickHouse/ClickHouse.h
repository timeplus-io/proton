#pragma once

#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/IExternalTable.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouse final : public IExternalTable
{
public:
    explicit ClickHouse(const String & name, ExternalTableSettingsPtr settings);

    void startup() override;
    void shutdown() override {}

    ColumnsDescription getTableStructure() override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t /*num_streams*/) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

private:
    ConnectionParameters connection_params;
    String database;
    String table;

    Poco::Logger * logger;
};

}

}
