#pragma once

#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>

#include <base/shared_ptr_helper.h>

namespace DB
{
struct ExternalStreamSettings;
class StorageExternalStreamImpl;

/// StorageExternalStream acts like a routing storage engine which proxy the requests to the underlying specific
/// external streaming storage like Kafka, Redpanda etc.
class StorageExternalStream final : public shared_ptr_helper<StorageExternalStream>, public IStorage, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalStream>;

public:
    std::string getName() const override { return "ExternalStream"; }

    void startup() override;
    void shutdown() override;
    bool supportsSubcolumns() const override;
    bool squashInsert() const noexcept override { return false; }
    bool supportsAccurateSeekTo() const noexcept override { return true; }
    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override;

    bool supportsStreamingQuery() const override { return true; }

    friend class KafkaSource;

    ExternalStreamCounterPtr getExternalStreamCounter();

protected:
    StorageExternalStream(
        const ASTs & engine_args,
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<ExternalStreamSettings> external_stream_settings_,
        const String & comment,
        bool attach);

private:
    std::unique_ptr<StorageExternalStreamImpl> external_stream;
};

}
