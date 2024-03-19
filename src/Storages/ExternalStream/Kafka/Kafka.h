#pragma once

#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Kafka/Consumer.h>
#include <Storages/ExternalStream/Kafka/ConsumerPool.h>
#include <Storages/ExternalStream/Kafka/Producer.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;


class Kafka final : public StorageExternalStreamImpl
{
public:
    using ConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;

    static Poco::Logger * cbLogger() {
        static Poco::Logger * logger { &Poco::Logger::get("KafkaExternalStream") };
        return logger;
    }

    static int onStats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque);
    static void onError(struct rd_kafka_s * rk, int err, const char * reason, void * opaque);
    static void onThrottle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque);

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
    Int32 topicRefreshIntervalMs() const { return topic_refresh_interval_ms; }
    const String & brokers() const { return settings->brokers.value; }
    const String & dataFormat() const override { return data_format; }
    const String & topic() const { return settings->topic.value; }
    const ASTPtr & shardingExprAst() const { assert(!engine_args.empty()); return engine_args[0]; }
    bool hasCustomShardingExpr() const;

    RdKafka::Producer & getProducer() const
    {
        assert(producer);
        return *producer;
    }
    RdKafka::ConsumerPool::Entry getConsumer() const
    {
        assert(consumer_pool);
        return consumer_pool->get(/*max_wait_ms=*/1000);
    }

    String getLoggerName() const { return storage_id.getDatabaseName() == "default" ? storage_id.getTableName() : storage_id.getFullNameNotQuoted(); }

private:
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const;
    void validateMessageKey(const String & message_key, IStorage * storage, const ContextPtr & context);
    void validate(const std::vector<int32_t> & shards_to_query = {});
    static std::vector<int32_t> parseShards(const std::string & shards_setting);

    StorageID storage_id;
    ASTs engine_args;
    String data_format;
    ExternalStreamCounterPtr external_stream_counter;

    NamesAndTypesList virtual_column_names_and_types;

    std::mutex shards_mutex;
    Int32 shards = 0;

    ASTPtr message_key_ast;
    Int32 topic_refresh_interval_ms = 0;

    ConfPtr conf;
    RdKafka::ConsumerPoolPtr consumer_pool;
    std::unique_ptr<RdKafka::Producer> producer;

    Poco::Logger * logger;
};

}
