#pragma once

#include "config.h"

#if USE_NATSIO

#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Common/Logger.h>

#include <nats.h>

#include <atomic>
#include <mutex>
#include <optional>

namespace DB
{

namespace ExternalStream
{

class NATSJetstreamSource;

class NATSJetstream final : public StorageExternalStreamImpl
{
    friend class NATSJetstreamSource;

public:
    NATSJetstream(
        StorageID storage_id,
        StorageInMemoryMetadata metadata,
        ExternalStreamSettingsPtr settings_,
        bool attach,
        ExternalStreamCounterPtr counter,
        ContextPtr context);

    ~NATSJetstream() override;

    String getName() const override { return "NATSJetstreamExternalStream"; }

    void startup() override;
    void shutdown(bool dropping) override;

    NamesAndTypesList getVirtuals() const override;
    std::optional<String> preferredColumn() const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    bool supportsSubcolumns() const override { return true; }

    /// Accessors used by source/sink
    const String & getUrl() const { return settings->url.value; }
    const String & getStreamName() const { return settings->stream_name.value; }
    const String & getSubject() const { return settings->subject.value; }
    const String & getConsumerName() const { return settings->consumer_name.value; }
    UInt64 getBatchSize() const { return settings->batch_size.value; }
    UInt64 getFetchTimeoutMs() const { return settings->fetch_timeout_ms.value; }
    bool isDurable() const { return settings->durable.value; }
    const String & getAckPolicy() const { return settings->ack_policy.value; }
    bool produceOneMessagePerRow() const { return settings->one_message_per_row.value; }
    UInt64 stallTimeoutMs() const { return settings->nats_stall_timeout_ms.value.totalMilliseconds(); }


    jsCtx * getJetStreamContext();
    natsConnection * getConnection();

    /// Creates a pull subscription. Caller owns the result.
    natsSubscription * createPullSubscription(const String & consumer_name_override, const ContextPtr & context);

private:
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void connect();
    void disconnect();

    void validateSettings(bool attach);
    void cacheVirtualColumnNamesAndTypes();

    static ExternalStreamSettingsPtr validateAndMoveSettings(ExternalStreamSettingsPtr && settings_);
    natsOptions * createConnectionOptions();


    natsConnection * connection = nullptr;
    jsCtx * js_context = nullptr;
    mutable std::mutex connection_mutex;


    NamesAndTypesList virtual_column_names_and_types;

    ExternalStreamCounterPtr external_stream_counter;
};

}

}

#endif
