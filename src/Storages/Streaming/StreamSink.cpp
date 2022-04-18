#include "StreamSink.h"
#include "StorageStream.h"

#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <KafkaLog/KafkaWAL.h>
#include <NativeLog/Server/NativeLog.h>
#include <base/ClockUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int INTERNAL_ERROR;
    extern const int OK;
}

StreamSink::StreamSink(StorageStream & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query_context(query_context_)
    , storage_id(storage.getStorageID())
    , request{storage_id.getTableName(), storage_id.uuid, storage.shard, nullptr}
{
    /// metadata_snapshot can contain only partial columns of the schema when light ingest feature is on
    /// Check this case here
    const auto & sink_block_header = getHeader();
    auto full_metadata_snapshot = storage_.getInMemoryMetadataPtr(metadata_snapshot->version);
    auto full_header = full_metadata_snapshot->getSampleBlockNonMaterialized();
    if (full_header.columns() != sink_block_header.columns())
    {
        /// light ingest
        assert(query_context->getSettingsRef().enable_light_ingest);

        column_positions.reserve(sink_block_header.columns());

        /// Figure out the column positions since we need encode them to file system
        for (const auto & col : sink_block_header)
            column_positions.push_back(full_header.getPositionByName(col.name));
    }


    if (!storage.kafka)
    {
        native_log = &nlog::NativeLog::instance(query_context);
        assert(native_log->enabled());
    }
}

BlocksWithShard StreamSink::doShardBlock(Block block) const
{
    auto selector = storage.createSelector(block);

    Blocks sharded_blocks(storage.shards);

    for (Int32 shard_idx = 0; shard_idx < storage.shards; ++shard_idx)
        sharded_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns sharded_columns = block.getByPosition(col_idx_in_block).column->scatter(storage.shards, selector);
        for (Int32 shard_idx = 0; shard_idx < storage.shards; ++shard_idx)
            sharded_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(sharded_columns[shard_idx]);
    }

    BlocksWithShard blocks_with_shard;

    /// Filter out empty blocks
    for (size_t shard_idx = 0; shard_idx < sharded_blocks.size(); ++shard_idx)
    {
        if (sharded_blocks[shard_idx].rows())
            blocks_with_shard.emplace_back(std::move(sharded_blocks[shard_idx]), shard_idx);

        assert(sharded_blocks[shard_idx].rows() == 0);
    }

    return blocks_with_shard;
}

BlocksWithShard StreamSink::shardBlock(Block block) const
{
    size_t shard = 0;
    if (storage.shards > 1)
    {
        if (storage.sharding_key_expr && !storage.rand_sharding_key)
            return doShardBlock(std::move(block));
        else
            /// Randomly pick one shard to ingest this block
            shard = storage.getNextShardIndex();
    }

    return {BlockWithShard{Block(std::move(block)), shard}};
}

inline IngestMode StreamSink::getIngestMode() const
{
    auto ingest_mode = query_context->getIngestMode();
    if (ingest_mode != IngestMode::None && ingest_mode != IngestMode::INVALID)
        return ingest_mode;

    if (storage.default_ingest_mode != IngestMode::None && ingest_mode != IngestMode::INVALID)
        return storage.default_ingest_mode;

    return IngestMode::ASYNC;
}

void StreamSink::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    assert(column_positions.empty() || column_positions.size() == chunk.getNumColumns());

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    /// 1) Split block by sharding key
    BlocksWithShard blocks{shardBlock(std::move(block))};

    auto ingest_mode = getIngestMode();

    /// 2) Commit each sharded block to corresponding streaming store partition
    /// we failed the whole insert whenever single block failed
    /// proton: FIXME. Once we MVCC schema, each block will be bound to a schema version, for now, it is 0.
    UInt16 schema_version = 0;
    const auto & idem_key = query_context->getIdempotentKey();
    auto record = std::make_shared<nlog::Record>(nlog::OpCode::ADD_DATA_BLOCK, Block{}, schema_version);
    record->setColumnPositions(column_positions);

    for (auto & current_block : blocks)
    {
        record->getBlock().swap(current_block.block);
        record->setShard(current_block.shard);

        if (!idem_key.empty())
            record->setIdempotentKey(idem_key);

        if (native_log)
            appendToNativeLog(record, ingest_mode);
        else
            appendToKafka(record, ingest_mode);
    }
}

void StreamSink::appendToNativeLog(nlog::RecordPtr & record, IngestMode /*ingest_mode*/)
{
    assert(native_log);

    request.stream_shard.shard = record->getShard();
    request.record = record;

    auto resp{native_log->append(storage_id.getDatabaseName(), request)};
    if (resp.hasError())
    {
        LOG_ERROR(storage.log, "Failed to append record to native log, error={}", resp.errString());
        throw DB::Exception(ErrorCodes::INTERNAL_ERROR, "Failed to append record to native log, error={}", resp.errString());
    }
}

void StreamSink::appendToKafka(nlog::RecordPtr & record, IngestMode ingest_mode)
{
    assert(storage.kafka);

    switch (ingest_mode)
    {
        case IngestMode::ASYNC: {
            //                LOG_TRACE(
            //                    storage.log,
            //                    "[async] write a block={} rows={} shard={} query_status_poll_id={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard,
            //                    query_context->getQueryStatusPollId());

            storage.appendAsync(*record, query_context->getBlockBaseId(), outstanding);
            break;
        }
        case IngestMode::SYNC: {
            //                LOG_TRACE(
            //                    storage.log,
            //                    "[sync] write a block={} rows={} shard={} committed={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard,
            //                    committed);

            auto ret = storage.kafka->log->append(*record, &StreamSink::writeCallback, this, storage.kafka->append_ctx);
            if (ret != 0)
                throw Exception("Failed to insert data sync", ret);

            break;
        }
        case IngestMode::FIRE_AND_FORGET: {
            //                LOG_TRACE(
            //                    storage.log,
            //                    "[fire_and_forget] write a block={} rows={} shard={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard);

            auto ret = storage.kafka->log->append(*record, nullptr, nullptr, storage.kafka->append_ctx);
            if (ret != 0)
                throw Exception("Failed to insert data fire_and_forget", ret);

            break;
        }
        case IngestMode::ORDERED: {
            auto ret = storage.kafka->log->append(*record, storage.kafka->append_ctx);
            if (ret.err != ErrorCodes::OK)
                throw Exception("Failed to insert data ordered", ret.err);

            break;
        }
        case IngestMode::None:
            /// FALLTHROUGH
        case IngestMode::INVALID:
            throw Exception("Failed to insert data, ingest mode is not setup", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    outstanding += 1;
}

void StreamSink::writeCallback(const klog::AppendResult & result)
{
    ++committed;
    if (result.err != ErrorCodes::OK)
        errcode = result.err;

    /// LOG_TRACE(storage.log, "[sync] written a block, and current committed={}, error={}", committed, errcode);
}

void StreamSink::writeCallback(const klog::AppendResult & result, void * data_)
{
    auto * stream = static_cast<StreamSink *>(data_);
    stream->writeCallback(result);
}

void StreamSink::onFinish()
{
    /// We need wait for all outstanding ingesting block committed
    /// before dtor itself. Otherwise the if the registered callback is invoked
    /// after dtor, crash will happen
    if (!storage.kafka || getIngestMode() != IngestMode::SYNC)
        return;

    /// 3) Inplace poll append result until either all of records have been committed
    auto start = MonotonicSeconds::now();
    while (1)
    {
        if (committed == outstanding)
        {
            /// LOG_DEBUG(storage.log, "[sync] write a block done, written blocks={}, committed={}, error={}", outstanding, committed, errcode);
            if (errcode != ErrorCodes::OK)
                throw Exception("Failed to insert data", errcode);

            return;
        }
        else
        {
            storage.kafka->log->poll(10, storage.kafka->append_ctx);
        }

        if (MonotonicSeconds::now() - start >= 2)
        {
            LOG_ERROR(
                storage.log, "Still Waiting for data to be committed. Appended data seems getting lost. There are probably having bugs.");
            start = MonotonicSeconds::now();
        }
    }
}
}
