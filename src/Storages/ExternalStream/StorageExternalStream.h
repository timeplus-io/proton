#pragma once

#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>
#include <Common/SettingsChanges.h>

namespace DB
{
struct ExternalStreamSettings;
class StorageExternalStreamImpl;

/// StorageExternalStream acts like a routing storage engine which proxy the requests to the underlying specific
/// external streaming storage like Kafka, Redpanda etc. This type of stream doesn't have any backing storage in Proton.
/// So they can service query only (ingest to this type of storage engine ends up with error)
class StorageExternalStream final : public shared_ptr_helper<StorageExternalStream>, public IStorage, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalStream>;

public:
    std::string getName() const override { return "ExternalStream"; }

    void startup() override;
    void shutdown() override;
    bool supportsSubcolumns() const override;
    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override;

    friend class KafkaSource;

protected:
    StorageExternalStream(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<ExternalStreamSettings> external_stream_settings_);

private:
    std::unique_ptr<StorageExternalStreamImpl> external_stream;
};

}
