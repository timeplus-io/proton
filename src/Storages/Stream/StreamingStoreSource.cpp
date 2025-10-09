#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/Stream/StreamingBlockReaderNativeLog.h>
#include <Storages/Stream/StreamingStoreSource.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<StreamShardStore> stream_shard_store_,
    const Block & header,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    Int64 sn,
    LoggerPtr logger_)
    : StreamingStoreSourceBase(header, storage_snapshot_, std::move(context_), logger_, ProcessorID::StreamingStoreSourceID)
    , stream_shard_store(stream_shard_store_)
{
    /// When read from the latest, we always want to get the exact sequence number to start from
    if (sn == cluster::Constants::LatestSN)
    {
        if (auto maybe_log_committed_sn = stream_shard_store_->committedSequence(); maybe_log_committed_sn)
            sn = std::max(*maybe_log_committed_sn + 1, cluster::Constants::LogStartSN);
    }

    if (sn >= cluster::Constants::LogStartSN)
        setLastProcessedSN(sn - 1);

    if (query_context->getSettingsRef().enable_idempotent_processing.value)
    {
        if (stream_shard_store_->isVirtualReplica())
            throw Exception(ErrorCodes::UNSUPPORTED, "Cannot enable idempotent processing on virtual replica");

        auto idem_keys = stream_shard_store_->getIdempotentKeysSnapshot(sn - 1, query_context->getSettingsRef().max_idempotent_ids);
        LOG_INFO(logger, "Set idempotent keys snapshot: max_sn={}, num_keys={}", sn - 1, idem_keys->size());
        StreamingStoreSourceBase::setIdempotentKeys(std::move(idem_keys));
    }

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;
    const auto & settings = query_context->getSettingsRef();
    if (settings.record_consume_batch_count.value != 0)
        record_consume_batch_count = static_cast<UInt32>(settings.record_consume_batch_count.value);

    if (settings.record_consume_timeout_ms.value != 0)
        record_consume_timeout_ms = static_cast<Int32>(settings.record_consume_timeout_ms.value);

    auto sample_block = storage_snapshot_->getMetadataForQuery()->getSampleBlock();
    if (stream_shard_store_->isNativeLog())
    {
        auto fetch_buffer_size = query_context->getSettingsRef().fetch_buffer_size;
        fetch_buffer_size = std::min<UInt64>(64 * 1024 * 1024, fetch_buffer_size);
        auto is_local_ = stream_shard_store_->isLocal();
        auto nlog_reader = std::make_unique<StreamingBlockReaderNativeLog>(
            sn,
            /*max_wait_ms_=*/record_consume_timeout_ms,
            /*max_bytes_=*/record_consume_batch_count * 8192,
            /*queued_max_bytes_=*/fetch_buffer_size,
            /*stream_shard_=*/stream_shard_store_->streamShard(),
            /*storage=*/&stream_shard_store_->storageStream(),
            /*sample_block=*/std::move(sample_block),
            /*query_schema_version=*/storage_snapshot_->getMetadataForQuery()->getVersion(),
            /*colum_positions_=*/columns_desc.physical_column_positions_to_read,
            /*is_stopped_=*/[shard_store = std::move(stream_shard_store_)] { return shard_store->isStopped(); },
            /*is_local_=*/is_local_,
            logger);
        nlog_reader->setAllowFallbackToHistoricalStore(settings.allow_fallback_to_historical_store.value);
        reader = std::move(nlog_reader);
    }
    else
    {
        // auto * kpool = Globals::getKafkaWALPool();
        // assert(kpool);
        // auto consumer = kpool->getOrCreateStreaming(stream_shard_store_->logStoreClusterId());
        // assert(consumer);
        // reader = std::make_unique<StreamingBlockReaderKafka>(
        //     sn,
        //     /*max_wait_ms_=*/record_consume_timeout_ms,
        //     /*max_count_=*/record_consume_batch_count,
        //     /*stream_shard_=*/stream_shard_store_->streamShard(),
        //     /*storage=*/&stream_shard_store_->storageStream(),
        //     /*sample_block=*/std::move(sample_block),
        //     /*query_schema_version=*/storage_snapshot_->getMetadataForQuery()->getVersion(),
        //     columns_desc.physical_column_positions_to_read,
        //     std::move(consumer),
        //     [shard_store = std::move(stream_shard_store_)] { return shard_store->isStopped(); },
        //     logger);
    }

    setDescription(reader->description());
}

void StreamingStoreSource::readAndProcess()
{
    if (reader->isCancelled())
    {
        cancel();
        return;
    }

    auto records = reader->read();
    if (records.empty())
        return;

    process(records);
}

std::pair<String, Int32> StreamingStoreSource::getStreamShard() const
{
    return reader->getStreamShard();
}

std::pair<Int64, Int64> StreamingStoreSource::sequenceRange() const
{
    if (stream_shard_store->isVirtualReplica())
        return reader->sequenceRange();
    else
        return stream_shard_store->sequenceRange();
}

void StreamingStoreSource::doResetStartSN(Int64 sn)
{
    if (sn >= cluster::Constants::LogStartSN)
    {
        reader->resetSequenceNumber(sn);
        LOG_INFO(logger, "Reset start sn={}", sn);

        if (query_context->getSettingsRef().enable_idempotent_processing.value)
        {
            if (stream_shard_store->isVirtualReplica())
                throw Exception(ErrorCodes::UNSUPPORTED, "Cannot enable idempotent processing on virtual replica");

            auto idem_keys = stream_shard_store->getIdempotentKeysSnapshot(sn - 1, query_context->getSettingsRef().max_idempotent_ids);
            LOG_INFO(logger, "Reset idempotent keys snapshot: max_sn={}, num_keys={}", sn - 1, idem_keys->size());
            StreamingStoreSourceBase::setIdempotentKeys(std::move(idem_keys));
        }
    }
}

}
