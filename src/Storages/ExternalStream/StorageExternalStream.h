#pragma once

#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/StorageProxy.h>

#include <base/shared_ptr_helper.h>

namespace DB
{
struct ExternalStreamSettings;

/// StorageExternalStream acts like a routing storage engine which proxy the requests to the underlying specific
/// external streaming storage like Kafka, Redpanda etc.
class StorageExternalStream final : public shared_ptr_helper<StorageExternalStream>, public StorageProxy, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalStream>;

public:
    ExternalStreamCounterPtr getExternalStreamCounter() { return external_stream_counter; }

    StoragePtr getNested() const override { return external_stream; }

    String getName() const override { return "ExternalStream"; }

    void startup() override { external_stream->startup(); }
    void shutdown() override { external_stream->shutdown(); }

    bool isRemote() const override { return external_stream->isRemote(); }
    bool supportsSubcolumns() const override { return external_stream->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return external_stream->supportsStreamingQuery(); }
    bool supportsAccurateSeekTo() const noexcept override { return external_stream->supportsAccurateSeekTo(); }
    bool squashInsert() const noexcept override { return external_stream->squashInsert(); }

    NamesAndTypesList getVirtuals() const override { return external_stream->getVirtuals(); }
    std::optional<UInt64> totalRows(const Settings & settings) const override { return external_stream->totalRows(settings); }

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

    friend class KafkaSource;

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
    ExternalStreamCounterPtr external_stream_counter;
    StoragePtr external_stream;
};

}
