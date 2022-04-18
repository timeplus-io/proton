#pragma once

#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

namespace DB
{

class IStorage;

class Kafka final : public StorageExternalStreamImpl
{
public:
    Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_);
    ~Kafka() override = default;

    void startup() override { }
    void shutdown() override { }
    bool supportsSubcolumns() const override { return true; }
    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    const String & brokers() const { return settings->brokers.value; }
    const String & dataFormat() const { return data_format; }
    const String & dataSchema() const { return settings->data_schema.value; }
    const String & topic() const { return settings->topic.value; }

private:
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const String & seek_to) const;

private:
    StorageID storage_id;
    std::unique_ptr<ExternalStreamSettings> settings;
    String data_format;

    Poco::Logger * log;

    NamesAndTypesList virtual_column_names_and_types;

    Int32 shards;
};
}
