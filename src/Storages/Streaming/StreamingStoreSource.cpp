#include "StreamingStoreSource.h"
#include "StreamShard.h"
#include "StreamingBlockReaderKafka.h"
#include "StreamingBlockReaderNativeLog.h"

#include <Interpreters/inplaceBlockConversions.h>
#include <KafkaLog/KafkaWALPool.h>
#include <Common/logger_useful.h>

namespace DB
{
StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<StreamShard> stream_shard_,
    const Block & header,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    Int64 sn,
    Poco::Logger * log_)
    : StreamingStoreSourceBase(header, storage_snapshot_, std::move(context_), log_, ProcessorID::StreamingStoreSourceID)
{
    const auto & settings = query_context->getSettingsRef();
    if (settings.record_consume_batch_count.value != 0)
        record_consume_batch_count = static_cast<UInt32>(settings.record_consume_batch_count.value);

    if (settings.record_consume_timeout_ms.value != 0)
        record_consume_timeout_ms = static_cast<Int32>(settings.record_consume_timeout_ms.value);

    if (stream_shard_->isLogStoreKafka())
    {
        auto & kpool = klog::KafkaWALPool::instance(query_context);
        assert(kpool.enabled());
        auto consumer = kpool.getOrCreateStreaming(stream_shard_->logStoreClusterId());
        assert(consumer);
        kafka_reader = std::make_unique<StreamingBlockReaderKafka>(
            std::move(stream_shard_), sn, columns_desc.physical_column_positions_to_read, std::move(consumer), log);
    }
    else
    {
        auto fetch_buffer_size = query_context->getSettingsRef().fetch_buffer_size;
        fetch_buffer_size = std::min<UInt64>(64 * 1024 * 1024, fetch_buffer_size);
        nativelog_reader = std::make_unique<StreamingBlockReaderNativeLog>(
            std::move(stream_shard_),
            sn,
            record_consume_timeout_ms,
            fetch_buffer_size,
            /*schema_provider*/ nullptr,
            /*schema_version*/ 0,
            columns_desc.physical_column_positions_to_read,
            log);
    }
}

String StreamingStoreSource::description() const
{
    String uuid;
    Int64 shard;
    if (nativelog_reader)
        std::tie(uuid, shard) = nativelog_reader->getStreamShard();
    else
        std::tie(uuid, shard) = kafka_reader->getStreamShard();

    return fmt::format("uuid={},shard={}", uuid, shard);
}

nlog::RecordPtrs StreamingStoreSource::read()
{
    if (nativelog_reader)
        return nativelog_reader->read();
    else
        return kafka_reader->read(record_consume_batch_count, record_consume_timeout_ms);
}

void StreamingStoreSource::readAndProcess()
{
    auto records = read();
    if (records.empty())
        return;

    result_chunks_with_sns.clear();
    result_chunks_with_sns.reserve(records.size());

    for (auto & record : records)
    {
        if (record->empty())
            continue;

        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->getBlock();
        auto rows = block.rows();

        assert(columns_desc.positions.size() >= block.columns());

        fillAndUpdateObjectsIfNecessary(block);

        for (const auto & pos : columns_desc.positions)
        {
            switch (pos.type())
            {
                case SourceColumnsDescription::ReadColumnType::PHYSICAL: {
                    columns.push_back(block.getByPosition(pos.physicalPosition()).column);
                    break;
                }
                case SourceColumnsDescription::ReadColumnType::VIRTUAL: {
                    /// The current column to return is a virtual column which needs be calculated lively
                    assert(columns_desc.virtual_col_calcs[pos.virtualPosition()]);
                    auto ts = columns_desc.virtual_col_calcs[pos.virtualPosition()](record);
                    /// NOTE: The `FilterTransform` will try optimizing filter ConstColumn to always_false or always_true,
                    /// for exmaple: `_tp_sn < 1`, if filter first data _tp_sn is 0, it will be optimized always_true.
                    /// So we can not create a constant column, since the virtual column data isn't constants value in fact.
                    auto virtual_column = columns_desc.virtual_col_types[pos.virtualPosition()]->createColumnConst(rows, ts)->convertToFullColumnIfConst();
                    columns.push_back(std::move(virtual_column));
                    break;
                }
                case SourceColumnsDescription::ReadColumnType::SUB: {
                    columns.push_back(
                        getSubcolumnFromBlock(block, pos.parentPosition(), columns_desc.subcolumns_to_read[pos.subPosition()]));
                    break;
                }
            }
        }

        result_chunks_with_sns.emplace_back(Chunk{std::move(columns), rows}, record->getSN());
        if (likely(block.info.appendTime() > 0))
        {
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setAppendTime(block.info.appendTime());
            result_chunks_with_sns.back().first.setChunkContext(std::move(chunk_ctx));
        }
    }
    iter = result_chunks_with_sns.begin();
}

std::pair<String, Int32> StreamingStoreSource::getStreamShard() const
{
    if (nativelog_reader)
        return nativelog_reader->getStreamShard();
    else
        return kafka_reader->getStreamShard();
}

void StreamingStoreSource::doResetStartSN(Int64 sn)
{
    if (sn >= 0)
    {
        if (nativelog_reader)
            nativelog_reader->resetSequenceNumber(sn);
        else
            kafka_reader->resetOffset(sn);
    }
}
}
