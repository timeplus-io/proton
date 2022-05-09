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
    ContextPtr query_context_)
    : StreamingStoreSourceBase(header, std::move(storage_snapshot_), std::move(query_context_))
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

    result_chunks.clear();
    result_chunks.reserve(records.size());

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

        if (hasObjectColumns())
            fillAndUpdateObjects(block);

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
                    assert(columns_desc.virtual_time_columns_calc[pos.virtualPosition()]);
                    auto ts = columns_desc.virtual_time_columns_calc[pos.virtualPosition()](block.info);
                    auto time_column = columns_desc.virtual_col_type->createColumnConst(rows, ts);
                    columns.push_back(std::move(time_column));
                    break;
                }
                case SourceColumnsDescription::ReadColumnType::SUB:
                {
                    /// need a deep copy
                    auto col{getSubcolumnFromblock(
                        block, pos.parentPosition(), columns_desc.subcolumns_to_read[pos.subPosition()])};
                    columns.push_back(col->cloneResized(col->size()));
                    break;
                }
            }
        }

        result_chunks.emplace_back(std::move(columns), rows);
        if (likely(block.info.append_time > 0))
        {
            auto chunk_info = std::make_shared<ChunkInfo>();
            chunk_info->ctx.setAppendTime(block.info.append_time);
            result_chunks.back().setChunkInfo(std::move(chunk_info));
        }
    }
    iter = result_chunks.begin();
}

void StreamingStoreSourceChannel::add(nlog::RecordPtrs records)
{
    auto added = records_queue.emplace(std::move(records));
    assert(added);
    (void)added;
}
}
