#pragma once

#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Kafka/Consumer.h>
#include <Storages/ExternalStream/Kafka/Producer.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;

class Kafka final : public StorageExternalStreamImpl
{
public:
    using ConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;

    static const String VIRTUAL_COLUMN_MESSAGE_KEY;

    static Poco::Logger * cbLogger() {
        static Poco::Logger * logger { &Poco::Logger::get("KafkaExternalStream") };
        return logger;
    }

    static int onStats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque);
    static void onError(struct rd_kafka_s * rk, int err, const char * reason, void * opaque);
    static void onThrottle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque);
    static void onLog(const struct rd_kafka_s * rk, int level, const char * fac, const char * buf);

    Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ASTs & engine_args_, bool attach, ExternalStreamCounterPtr external_stream_counter_, ContextPtr context);
    ~Kafka() override = default;

    void startup() override { LOG_INFO(logger, "Starting Kafka External Stream"); }
    void shutdown() override {
        LOG_INFO(logger, "Shutting down Kafka External Stream");

        /// Must release all resources here rather than relying on the deconstructor.
        /// Because the `Kafka` instance will not be destroyed immediately when the external stream gets dropped.
        {
            std::lock_guard<std::mutex> lock{consumer_mutex};
            for (const auto & consumer_ptr : consumers)
                /// if the consumer is still running, mark it stopped
                if (auto consumer = consumer_ptr.lock())
                    consumer->setStopped();

            consumers.clear();
        }

        if (producer)
            producer->setStopped();

        if (producer_topic)
            producer_topic.reset();

        if (producer)
            producer.reset();

        tryRemoveTempDir(logger);
    }
    bool supportsSubcolumns() const override { return true; }
    NamesAndTypesList getVirtuals() const override;
    ExternalStreamCounterPtr getExternalStreamCounter() const override { return external_stream_counter; }

    std::optional<UInt64> totalRows(const Settings &) override;

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
    const String & topicName() const { return settings->topic.value; }
    const ASTPtr & shardingExprAst() const { assert(!engine_args.empty()); return engine_args[0]; }
    bool hasCustomShardingExpr() const;

    std::shared_ptr<RdKafka::Producer> getProducer();
    std::shared_ptr<RdKafka::Topic> getProducerTopic();
    std::shared_ptr<RdKafka::Consumer> getConsumer();

    String getLoggerName() const { return storage_id.getDatabaseName() == "default" ? storage_id.getTableName() : storage_id.getFullNameNotQuoted(); }

private:
    Kafka::ConfPtr createRdConf(KafkaExternalStreamSettings settings_);
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const RdKafka::Consumer & consumer, const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const;
    void validateMessageKey(const String & message_key, IStorage * storage, const ContextPtr & context);
    void validate();

    ASTs engine_args;
    String data_format;
    ExternalStreamCounterPtr external_stream_counter;

    NamesAndTypesList virtual_column_names_and_types;

    ASTPtr message_key_ast;
    Int32 topic_refresh_interval_ms = 0;
    std::vector<Int32> shards_from_settings;
    fs::path broker_ca_file;

    bool support_count_optimization = false;

    ConfPtr conf;
    UInt64 poll_timeout_ms = 0;

    /// The Producer instance and Topic instance can be used by multiple sinks at the same time, thus we only need one of each.
    std::mutex producer_mutex;
    std::shared_ptr<RdKafka::Producer> producer;
    std::shared_ptr<RdKafka::Topic> producer_topic;

    /// A Consumer can only be used by one source at the same time (technically speaking, it can be used by multple sources as long as each source read from a different topic,
    /// but we will leave this as an enhancement later, probably when we introduce the `Connection` concept), thus we need a consumer pool.
    std::mutex consumer_mutex;
    size_t max_consumers = 0;
    std::vector<std::weak_ptr<RdKafka::Consumer>> consumers;

    Poco::Logger * logger;
};

}
