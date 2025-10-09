#pragma once

#include <ClickHouse/Client.h>
#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/StreamIOStats.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouse final : public StorageExternalTable, public DB::ClickHouse::ITypeNameProvider
{
public:
    static constexpr uint64_t DEFAULT_CONNECTION_POOL_SIZE = 3000;

    explicit ClickHouse(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        std::unique_ptr<ExternalTableSettings> settings_,
        bool attach_,
        const ContextPtr & context_);

    std::string getType() const override { return "ClickHouse"; }

    const std::unordered_map<String, String> & getColumnTypeNames() const override { return original_column_type_names; }

    Int64 getMetricTime(bool reset = true) override;
    UInt64 readBytes(bool reset = true, bool external_ingress = true) override;
    UInt64 readRows(bool reset = true, bool external_ingress = true) override;
    UInt64 writtenBytes(bool reset = true, bool external_ingress = true) override;
    UInt64 writtenRows(bool reset = true, bool external_ingress = true) override;

private:
    void getTableSchema(ContextPtr, ColumnsDescription &) override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t /*num_streams*/) override;

    SinkToStoragePtr writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    std::unique_ptr<DB::ClickHouse::Client> createClient(const ContextPtr & local_context, bool use_dedicated_connection = false) const;

    DB::ClickHouse::ConnectionPoolPtr pool;
    ConnectionTimeouts timeouts;
    String database;
    String table;
    /// The type names fetched from the ClickHouse server
    std::unordered_map<String, String> original_column_type_names;

    StreamIOStats metrics{MonotonicMilliseconds::now()};

    LoggerPtr logger;
};

}

}
