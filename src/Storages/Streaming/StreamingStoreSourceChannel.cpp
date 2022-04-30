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
        /// `virtual_columns_pos_begin` is also means total physical columns in schema
        assert(block.columns() == virtual_columns_pos_begin);

        if (hasObjectColumns())
            fillAndUpdateObjects(block);

        /// Pos Range: [0, ..., virtual_columns_pos_begin, ..., subcolumns_pos_begin, ...)
        for (size_t subcolumn_index = 0; auto pos : column_positions)
        {
            if (pos < virtual_columns_pos_begin)
            {
                /// We can't move the column to columns
                /// since the records can be shared among multiple threads
                /// We need a deep copy
                auto col{block.getByPosition(pos).column};
                columns.push_back(col->cloneResized(col->size()));
            }
            else if (pos >= subcolumns_pos_begin)
            {
                /// It's a subcolumn, the parent (physical) column pos in schema is `pos - subcolumns_pos_begin`
                columns.push_back(getSubcolumnFromblock(block, pos - subcolumns_pos_begin, subcolumns_to_read[subcolumn_index]));
                ++subcolumn_index;
            }
            else
            {
                assert(virtual_time_columns_calc[pos - virtual_columns_pos_begin]);
                auto ts = virtual_time_columns_calc[pos - virtual_columns_pos_begin](block.info);
                auto time_column = virtual_col_type->createColumnConst(rows, ts);
                columns.push_back(std::move(time_column));
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
