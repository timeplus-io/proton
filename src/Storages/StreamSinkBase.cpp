#include <Storages/StreamSinkBase.h>
#include <Storages/Stream/StorageStream.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Common/ErrorCodes.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <base/ClockUtils.h>
#include <base/sleep.h>

#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int TIMEOUT_EXCEEDED;
extern const int NOT_A_LEADER;
extern const int STREAM_PAUSED;
extern const int TOO_LARGE_RECORD;
extern const int UNKNOWN_STREAM;
}

StreamSinkBase::StreamSinkBase(
    StoragePtr storage_,
    StorageMetadataPtr metadata_snapshot_,
    IngestMode ingest_mode_,
    UInt32 shards_,
    ContextPtr query_context_,
    LoggerPtr logger_,
    ProcessorID pid_)
    : SinkToStorage(
          query_context_->getSettingsRef().insert_allow_materialized_columns.value ? metadata_snapshot_->getSampleBlock()
                                                                                   : metadata_snapshot_->getSampleBlockNonMaterialized(),
          pid_)
    , storage(std::move(storage_))
    , metadata_snapshot(metadata_snapshot_)
    , query_context(std::move(query_context_))
    , ingest_mode(ingest_mode_)
    , shards(shards_)
    , versioned_column_positions(cluster::Nulls::NullVersion, {})
    , chunk_splitter(
          getMaxInsertBlockRows(query_context->getSettingsRef()),
          getMaxInsertBlockBytes(query_context->getSettingsRef(), *storage))
    , logger(logger_ ? std::move(logger_) : getLogger("StreamSinkBase"))
{
    const auto & settings = query_context->getSettingsRef();
    append_timeout_ms = settings.insert_timeout_ms.value;
    max_retries = settings.insert_max_tries;
    retry_initial_backoff_ms = settings.insert_retry_initial_backoff_ms;
    retry_max_backoff_ms = settings.insert_retry_max_backoff_ms;
}

void StreamSinkBase::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto current_chunk_id = chunk_id++;
    const auto & idem_key = query_context->getIdempotentKey();

    UInt16 schema_version = metadata_snapshot->getVersion();
    const auto & column_positions = getColumnPositions(schema_version);
    assert(column_positions.empty() || column_positions.size() == chunk.getNumColumns());

    /// 1) Split block by sharding key and max block size/bytes.
    auto sharded_blocks = shardBlock(getHeader().cloneWithColumns(chunk.detachColumns()));

    /// Group split blocks by shard so same shard blocks are ingested in order.
    std::vector<std::vector<Block>> blocks_by_shard(shards);
    size_t num_shards_with_blocks = 0;
    for (auto & bs : sharded_blocks)
    {
        if (blocks_by_shard[bs.shard].empty())
            ++num_shards_with_blocks;
        blocks_by_shard[bs.shard].push_back(std::move(bs.block));
    }

    size_t shard = 0;
    const auto & settings = query_context->getSettingsRef();
    std::optional<ThreadPool> pool;

    if (num_shards_with_blocks > 1 && settings.enable_concurrent_ingest.value)
    {
        /// Parallel ingest
        size_t threads = settings.max_insert_threads.value;
        if (threads == 0)
            threads = std::min<size_t>(num_shards_with_blocks - 1, 8 - 1); /// -1 for current ingest thread
        else
            threads = std::min(num_shards_with_blocks - 1, threads - 1);

        pool.emplace(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, threads, threads, /*max_queue_size_=*/num_shards_with_blocks);

        const size_t current_thread_share = (num_shards_with_blocks + threads) / (threads + 1);
        assert(current_thread_share < num_shards_with_blocks);

        for (size_t remaining_shards = num_shards_with_blocks; shard < blocks_by_shard.size() && remaining_shards > current_thread_share;
             ++shard)
        {
            if (!blocks_by_shard[shard].empty())
            {
                --remaining_shards;
                pool->scheduleOrThrow([this,
                                       shard,
                                       schema_version,
                                       &idem_key,
                                       current_chunk_id,
                                       &column_positions,
                                       blocks = std::move(blocks_by_shard[shard]),
                                       thread_group = CurrentThread::getGroup()]() mutable {
                    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);

                    for (size_t block_id = 0; block_id < blocks.size(); ++block_id)
                    {
                        auto record = std::make_shared<cluster::SchemaRecord>(
                            cluster::protocol::OpCode::InsertData, std::move(blocks[block_id]), schema_version);
                        record->setColumnPositions(column_positions);
                        record->setShard(static_cast<uint32_t>(shard));

                        if (!idem_key.empty())
                            record->setIdempotentKey(getBlockIdempotentKey(idem_key, current_chunk_id, block_id));

                        ++ingest_state.total_blocks;
                        if (max_retries > 0 && ingest_mode == IngestMode::Sync)
                        {
                            appendWithRetry(record);
                        }
                        else
                        {
                            auto append_result = appendLog(record, append_timeout_ms);
                            if (append_result.hasError())
                            {
                                ++ingest_state.failed_blocks;
                                ingest_state.last_errcode = append_result.err.error_code;
                            }
                            else
                            {
                                ++ingest_state.committed_blocks;
                            }
                        }
                    }
                });
            }
        }
        /// Fall through to sequential ingestion for the current thread
    }

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::InsertData, Block{}, schema_version);
    record->setColumnPositions(column_positions);

    for (const auto total_shards = blocks_by_shard.size(); shard < total_shards; ++shard)
    {
        auto & blocks = blocks_by_shard[shard];
        for (size_t block_id = 0, block_num = blocks.size(); block_id < block_num; ++block_id)
        {
            record->getBlock().swap(blocks[block_id]);
            record->setShard(static_cast<uint32_t>(shard));

            if (!idem_key.empty())
                record->setIdempotentKey(getBlockIdempotentKey(idem_key, current_chunk_id, block_id));

            ++ingest_state.total_blocks;
            if (max_retries > 0 && ingest_mode == IngestMode::Sync)
            {
                appendWithRetry(record);
            }
            else
            {
                auto append_result = appendLog(record, append_timeout_ms);
                if (append_result.hasError())
                {
                    ++ingest_state.failed_blocks;
                    ingest_state.last_errcode = append_result.err.error_code;
                }
                else
                {
                    ++ingest_state.committed_blocks;
                }
            }
        }
    }

    if (pool)
        pool->wait();

    if (ingest_state.hasError())
        throw DB::Exception(
            ingest_state.last_errcode, "Failed to ingest data, error_message='{}'", DB::ErrorCodes::getName(ingest_state.last_errcode));
}

void StreamSinkBase::appendWithRetry(cluster::SchemaRecordPtr & record)
{
    assert(ingest_mode == IngestMode::Sync && max_retries > 0);

    uint64_t backoff_ms = retry_initial_backoff_ms;
    int errcode = ErrorCodes::OK;

    for (uint64_t i = 0; i < max_retries; ++i)
    {
        auto append_result = appendLog(record, append_timeout_ms);
        errcode = append_result.err.error_code;
        if (!append_result.hasError() || !isErrorRetryable(append_result.err.error_code) || isCancelled())
            break;

        sleepForMilliseconds(backoff_ms);
        backoff_ms = std::min(backoff_ms * 2, retry_max_backoff_ms);
    }

    if (errcode == ErrorCodes::OK)
    {
        ++ingest_state.committed_blocks;
    }
    else
    {
        ++ingest_state.failed_blocks;
        ingest_state.last_errcode = errcode;
    }
}

void StreamSinkBase::onFinish()
{
    if (ingest_state.hasError())
        throw DB::Exception(
            ingest_state.last_errcode, "Failed to ingest data, error_message='{}'", DB::ErrorCodes::getName(ingest_state.last_errcode));
}

void StreamSinkBase::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    UInt64 timeout_ms = append_timeout_ms * 2;

    if (ingest_state.hasOutstandingBlocks())
        LOG_INFO(
            logger,
            "Waiting for outstanding blocks to commit, total_blocks={} committed_blocks={} failed_blocks={} last_errcode={}",
            ingest_state.total_blocks,
            ingest_state.committed_blocks,
            ingest_state.failed_blocks,
            ingest_state.last_errcode);

    Stopwatch stopwatch;
    uint64_t sleep_count = 0;

    while (!isCancelled())
    {
        if (!ingest_state.hasOutstandingBlocks())
        {
            ckpt_ctx->coordinator->checkpointed(getVersion(), getLogicID(), ckpt_ctx);

            if (sleep_count > 0)
            {
                LOG_INFO(
                    logger,
                    "Took {}ms to wait for outstanding blocks to commit total_blocks={} committed_blocks={} failed_blocks={}",
                    stopwatch.elapsedMilliseconds(),
                    ingest_state.total_blocks,
                    ingest_state.committed_blocks,
                    ingest_state.failed_blocks);
            }
            break;
        }

        if (ingest_state.hasError())
            throw Exception(
                ingest_state.last_errcode,
                "Failed to checkpoint, appended data has error, error_msg={}",
                DB::ErrorCodes::getName(ingest_state.last_errcode));

        if (auto waited_ms = stopwatch.elapsedMilliseconds(); waited_ms >= timeout_ms)
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout for checkpoint to wait for outstanding blocks to commit, waited_ms={} total_blocks={} "
                "committed_blocks={} failed_blocks={} last_errcode={}.",
                waited_ms,
                ingest_state.total_blocks,
                ingest_state.committed_blocks,
                ingest_state.failed_blocks,
                ingest_state.last_errcode);

        sleepForMilliseconds(20);

        if (++sleep_count % 50 == 0)
            LOG_INFO(
                logger,
                "Waiting for outstanding blocks to commit, waited_ms={} total_blocks={} committed_blocks={} failed_blocks={} "
                "last_errcode={}",
                stopwatch.elapsedMilliseconds(),
                ingest_state.total_blocks,
                ingest_state.committed_blocks,
                ingest_state.failed_blocks,
                ingest_state.last_errcode);
    }
}

std::string StreamSinkBase::getBlockIdempotentKey(const std::string & idempotent_key, UInt64 chunk_id_, size_t block_id_)
{
    return fmt::format("{}-{}-{}", idempotent_key, chunk_id_, block_id_);
}

UInt64 StreamSinkBase::getMaxInsertBlockRows(const Settings & settings)
{
    return settings.max_insert_block_size.value;
}

UInt64 StreamSinkBase::getMaxInsertBlockBytes(const Settings & settings, const IStorage & storage)
{
    if (settings.max_insert_block_bytes.changed)
        return settings.max_insert_block_bytes.value;

    if (const auto * stream_storage = dynamic_cast<const StorageStream *>(&storage))
        return stream_storage->maxEntrySize();

    return settings.max_insert_block_bytes.value;
}

bool StreamSinkBase::isErrorRetryable(int error_code)
{
    static const std::unordered_set<int> non_retryable_errors = {
        ErrorCodes::UNKNOWN_STREAM,
        ErrorCodes::STREAM_PAUSED,
        ErrorCodes::TOO_LARGE_RECORD,
        ErrorCodes::NOT_A_LEADER,
    };

    return !non_retryable_errors.contains(error_code);
}

const std::vector<UInt16> & StreamSinkBase::getColumnPositions(UInt16 version_requested)
{
    auto & [version, column_positions] = versioned_column_positions;
    if (version_requested == version) [[likely]]
        return column_positions;

    version = version_requested;

    if (!column_positions.empty())
        return column_positions;

    assert(column_positions.empty());
    const auto & sink_block_header = getHeader();
    auto full_metadata_snapshot = storage->getInMemoryMetadataByVersion(version_requested);
    auto full_header = query_context->getSettingsRef().insert_allow_materialized_columns.value
        ? full_metadata_snapshot->getSampleBlock()
        : full_metadata_snapshot->getSampleBlockNonMaterialized();
    if (full_header.columns() != sink_block_header.columns())
    {
        assert(query_context->getSettingsRef().enable_light_ingest);

        column_positions.reserve(sink_block_header.columns());
        for (const auto & col : sink_block_header)
            column_positions.push_back(full_header.getPositionByName(col.name));
    }

    return column_positions;
}

}
