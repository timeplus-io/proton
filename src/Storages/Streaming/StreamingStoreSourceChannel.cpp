#include "StreamingStoreSourceChannel.h"
#include "StreamingStoreSourceMultiplexer.h"

#include <DataTypes/ObjectUtils.h>
#include <Interpreters/inplaceBlockConversions.h>

namespace DB
{
StreamingStoreSourceChannel::StreamingStoreSourceChannel(
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
    Block header,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr query_context_,
    Poco::Logger * log_)
    : StreamingStoreSourceBase(header, storage_snapshot_, std::move(query_context_), log_, ProcessorID::StreamingStoreSourceChannelID) /// NOLINT(performance-move-const-arg)
    , id(sequence_id++)
    , multiplexer(std::move(multiplexer_))
    , records_queue(1000)
{
}

std::atomic<uint32_t> StreamingStoreSourceChannel::sequence_id = 0;

StreamingStoreSourceChannel::~StreamingStoreSourceChannel()
{
    multiplexer->removeChannel(id);
}

String StreamingStoreSourceChannel::description() const
{
    auto [uuid, shard] = multiplexer->getStreamShard();
    return fmt::format("id={},uuid={},shard={}", id, uuid, shard);
}

void StreamingStoreSourceChannel::readAndProcess()
{
    nlog::RecordPtrs records;
    auto got_records = records_queue.tryPop(records, 100);
    if (!got_records)
        return;

    /// Got shutdown signal
    if (records.empty())
    {
        cancel();
        return;
    }

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

        /// Block in channel shall always contain full columns
        assert(block.columns() == columns_desc.positions.size());

        fillAndUpdateObjectsIfNecessary(block);

        for (const auto & pos : columns_desc.positions)
        {
            switch (pos.type())
            {
                case SourceColumnsDescription::ReadColumnType::PHYSICAL:
                {
                    /// We can't move the column to columns
                    /// since the records can be shared among multiple threads
                    /// We need a deep copy
                    auto col{block.getByPosition(pos.physicalPosition()).column};
                    columns.push_back(col->cloneResized(col->size()));
                    break;
                }
                case SourceColumnsDescription::ReadColumnType::VIRTUAL:
                {
                    /// The current column to return is a virtual column which needs be calculated lively
                    assert(columns_desc.virtual_col_calcs[pos.virtualPosition()]);
                    auto ts = columns_desc.virtual_col_calcs[pos.virtualPosition()](record);
                    auto time_column = columns_desc.virtual_col_types[pos.virtualPosition()]->createColumnConst(rows, ts);
                    columns.push_back(std::move(time_column));
                    break;
                }
                case SourceColumnsDescription::ReadColumnType::SUB:
                {
                    /// need a deep copy
                    auto col{getSubcolumnFromBlock(block, pos.parentPosition(), columns_desc.subcolumns_to_read[pos.subPosition()])};
                    columns.push_back(col->cloneResized(col->size()));
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

void StreamingStoreSourceChannel::add(nlog::RecordPtrs records)
{
    [[maybe_unused]] auto added = records_queue.emplace(std::move(records));
    assert(added);
}

std::pair<String, Int32> StreamingStoreSourceChannel::getStreamShard() const
{
    return multiplexer->getStreamShard();
}
}
