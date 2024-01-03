#pragma once

#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;

class Kafka final : public StorageExternalStreamImpl
{
public:
    Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ASTs & engine_args_, bool attach, ExternalStreamCounterPtr external_stream_counter_, ContextPtr context);
    ~Kafka() override = default;

    void startup() override { }
    void shutdown() override { }
    bool supportsSubcolumns() const override { return true; }
    NamesAndTypesList getVirtuals() const override;
    ExternalStreamCounterPtr getExternalStreamCounter() const override { return external_stream_counter; }

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
    const String & dataFormat() const override { return data_format; }
    const String & topic() const { return settings->topic.value; }
    const klog::KConfParams & properties() const { return kafka_properties; }
    const klog::KafkaWALAuth & auth() const noexcept { return *auth_info; }
    const ASTPtr & shardingExprAst() const { assert(!engine_args.empty()); return engine_args[0]; }
    bool hasCustomShardingExpr() const;
    klog::KafkaWALSimpleConsumerPtr getConsumer(int32_t fetch_wait_max_ms = 200) const;

private:
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const;
    void validateMessageKey(const String & message_key, IStorage * storage, const ContextPtr & context);
    void validate(const std::vector<int32_t> & shards_to_query = {});
    static std::vector<int32_t> parseShards(const std::string & shards_setting);

    StorageID storage_id;
    ASTs engine_args;
    klog::KConfParams kafka_properties;
    String data_format;
    const std::unique_ptr<klog::KafkaWALAuth> auth_info;
    ExternalStreamCounterPtr external_stream_counter;
    Poco::Logger * log;

    NamesAndTypesList virtual_column_names_and_types;

    std::mutex shards_mutex;
    int32_t shards = 0;

    ASTPtr message_key_ast;
};
}
