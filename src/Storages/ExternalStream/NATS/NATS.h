#pragma once

#include <nats/nats.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{

class IStorage;

class NATS final : public StorageExternalStreamImpl
{
public:
    using ConfPtr = std::unique_ptr<natsOptions, decltype(natsOptions_Destroy) *>;

    static const String VIRTUAL_COLUMN_MESSAGE_KEY;

    NATS(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ASTs & engine_args_, bool attach, ExternalStreamCounterPtr external_stream_counter_, ContextPtr context);
    ~NATS() override = default;

    void startup() override { LOG_INFO(logger, "Starting NATS External Stream"); }
    void shutdown() override {
        LOG_INFO(logger, "Shutting down NATS External Stream");

        std::lock_guard<std::mutex> lock{consumer_mutex};
        for (const auto & consumer_ptr : consumers)
            if (auto consumer = consumer_ptr.lock())
                consumer->setStopped();

        consumers.clear();

        if (producer)
            producer->setStopped();

        if (producer_topic)
            producer_topic.reset();

        if (producer)
            producer.reset();
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
    const String & dataFormat() const override { return data_format; }
    const String & topicName() const { return settings->nats_subject.value; }
    const ASTPtr & shardingExprAst() const { assert(!engine_args.empty()); return engine_args[0]; }
    bool hasCustomShardingExpr() const;

    std::shared_ptr<NATSProducer> getProducer();
    std::shared_ptr<NATSTopic> getProducerTopic();
    std::shared_ptr<NATSConsumer> getConsumer();

    String getLoggerName() const { return storage_id.getDatabaseName() == "default" ? storage_id.getTableName() : storage_id.getFullNameNotQuoted(); }

private:
    NATS::ConfPtr createNATSConf(NATSExternalStreamSettings settings_);
    void calculateDataFormat(const IStorage * storage);
    void cacheVirtualColumnNamesAndTypes();
    std::vector<Int64> getOffsets(const NATSConsumer & consumer, const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const;
    void validate();

    ASTs engine_args;
    String data_format;
    ExternalStreamCounterPtr external_stream_counter;

    NamesAndTypesList virtual_column_names_and_types;

    Int32 topic_refresh_interval_ms = 0;
    std::vector<Int32> shards_from_settings;

    bool support_count_optimization = false;

    NATS::ConfPtr conf;

    std::mutex producer_mutex;
    std::shared_ptr<NATSProducer> producer;
    std::shared_ptr<NATSTopic> producer_topic;

    std::mutex consumer_mutex;
    size_t max_consumers = 0;
    std::vector<std::weak_ptr<NATSConsumer>> consumers;

    Poco::Logger * logger;
};

}
