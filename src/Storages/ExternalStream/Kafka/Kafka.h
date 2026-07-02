#pragma once

#include <Formats/KafkaSchemaRegistryForAvro.h>
#include <IO/Kafka/Connection.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/SeekToInfo.h>
#include <Common/Logger.h>

namespace DB
{

class IStorage;

namespace ExternalStream
{

class Kafka final : public StorageExternalStreamImpl
{
public:
    using ConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;

    static LoggerPtr cbLogger()
    {
        static LoggerPtr logger{getLogger("KafkaExternalStream")};
        return logger;
    }

    static int onStats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque);
    static void onError(struct rd_kafka_s * rk, int err, const char * reason, void * opaque);
    static void onThrottle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque);
    static void onLog(const struct rd_kafka_s * rk, int level, const char * fac, const char * buf);

    Kafka(
        StorageID storage_id,
        StorageInMemoryMetadata storage_metadata,
        std::unique_ptr<ExternalStreamSettings> settings_,
        ASTs engine_args_,
        bool attach,
        ExternalStreamCounterPtr external_stream_counter_,
        ContextPtr context);
    ~Kafka() override = default;

    String getName() const override { return "KafkaExternalStream"; }

    void startup() override;
    void shutdown(bool dropping) override;

    bool supportsAccurateSeekTo() const noexcept override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool squashInsert() const noexcept override { return false; }
    NamesAndTypesList getVirtuals() const override;
    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<String> preferredColumn() const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    bool produceOneMessagePerRow() const { return settings->one_message_per_row; }
    Int32 topicRefreshIntervalMs() const { return topic_refresh_interval_ms; }
    const String & brokers() const { return settings->brokers.value; }
    const String & topicName() const { return settings->topic.value; }
    const ASTPtr & shardingExprAst() const
    {
        assert(!engine_args.empty());
        return engine_args[0];
    }
    bool hasCustomShardingExpr() const;

    std::vector<int64_t> getLastSNs() const override;

    const String & secureProtocol() const { return settings->security_protocol.value; }
    const String & saslMechanism() const { return settings->sasl_mechanism.value; }
    bool hasSchemaRegistryUrl() const { return !settings->kafka_schema_registry_url.value.empty(); }
    bool skipSslCertCheck() const { return settings->skip_ssl_cert_check.value; }
    bool hasSslCaCertFile() const { return !settings->ssl_ca_cert_file.value.empty(); }
    bool hasSslCaPem() const { return !settings->ssl_ca_pem.value.empty(); }

    void validate(const ContextPtr & context) const override;
    void validateSettings(const ExternalStreamSettingsPtr & new_settings, bool change_settings, const ContextPtr & context_) const override;
    void validateColumns() const;

private:
    DB::Kafka::Conf createConf(KafkaExternalStreamSettings settings_);
    void cacheVirtualColumnNamesAndTypes();

    std::vector<Int64> getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<uint64_t> & shards_to_query) const;


    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    ASTs engine_args;
    ExternalStreamCounterPtr external_stream_counter;

    NamesAndTypesList virtual_column_names_and_types;

    Int32 topic_refresh_interval_ms = 0;
    std::vector<Int32> shards_from_settings;
    fs::path broker_ca_file;

    /// `client` is reset to nullptr in `shutdown()` to release the underlying
    /// rdkafka resources promptly (the Kafka instance itself can outlive
    /// shutdown by some time, see comment in shutdown()). Reads happen
    /// concurrently from materialized-view pipelines that may be mid-flight
    /// in `read()` etc. Always go through `getClient()` to load atomically
    /// and reject calls after shutdown.
    DB::Kafka::ConnectionPtr client;

    /// Atomically load `client` and throw ABORTED if the stream has been
    /// shut down (i.e. `client` was reset). Returning the loaded shared_ptr
    /// by value bumps the refcount so the Connection stays alive for the
    /// duration of whatever the caller does with it, even if a concurrent
    /// `shutdown()` resets the member.
    DB::Kafka::ConnectionPtr getClient() const;

    UInt64 poll_timeout_ms = 0;

    std::shared_ptr<KafkaSchemaRegistryForAvro> avro_key_schema_registry;
};

}

}
