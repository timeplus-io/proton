#pragma once

#include <KafkaLog/KafkaWALCommon.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;

class Kafka final : public StorageExternalStreamImpl
{
public:
    Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ASTs & engine_args_, bool attach);
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

    bool produceOneMessagePerRow() const { return settings->one_message_per_row; }
    const String & brokers() const { return settings->brokers.value; }
    const String & dataFormat() const { return data_format; }
    const String & dataSchema() const { return settings->data_schema.value; }
    const String & topic() const { return settings->topic.value; }
    const String & securityProtocol() const { return settings->security_protocol.value; }
    const String & username() const { return settings->username.value; }
    const String & password() const { return settings->password.value; }
    const String & sslCaCertFile() const { return settings->ssl_ca_cert_file.value; }
    const klog::KConfParams & properties() const { return kafka_properties; }
    bool hasCustomShardingExpr() const { return !engine_args.empty(); }
    const ASTPtr & shardingExprAst() const { assert(!engine_args.empty()); return engine_args[0]; }

private:
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const;
    void validate(const std::vector<int32_t> & shards_to_query = {});

    static std::vector<int32_t> parseShards(const std::string & shards_setting);

private:
    StorageID storage_id;
    std::unique_ptr<ExternalStreamSettings> settings;
    String data_format;

    Poco::Logger * log;

    NamesAndTypesList virtual_column_names_and_types;
    klog::KConfParams kafka_properties;
    const ASTs engine_args;

    std::mutex shards_mutex;
    int32_t shards = 0;
};
}
