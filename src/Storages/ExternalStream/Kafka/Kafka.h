#pragma once

#include <vector>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;

class Kafka final : public StorageExternalStreamImpl
{
public:
    Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, bool attach);
    ~Kafka() override = default;

    void startup() override { }
    void shutdown() override { }
    bool supportsSubcolumns() const override { return true; }
    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    const String & brokers() const { return settings->brokers.value; }
    const String & dataFormat() const { return data_format; }
    const String & dataSchema() const { return settings->data_schema.value; }
    const String & topic() const { return settings->topic.value; }
    const String & securityProtocol() const { return settings->security_protocol.value; }
    const String & username() const { return settings->username.value; }
    const String & password() const { return settings->password.value; }
    std::vector<std::pair<std::string, std::string>> properties() const { return parseProperties(settings->properties.value); }

private:
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info) const;
    void validate();
    static std::vector<std::pair<std::string, std::string>> parseProperties(std::string properties);

private:
    StorageID storage_id;
    std::unique_ptr<ExternalStreamSettings> settings;
    String data_format;

    Poco::Logger * log;

    NamesAndTypesList virtual_column_names_and_types;

    Int32 shards = -1;
};
}
