#include "StreamingStoreSourceChannel.h"
#include "StreamingStoreSourceMultiplexer.h"

namespace DB
{
StreamingStoreSourceChannel::StreamingStoreSourceChannel(
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
    Block header,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr query_context_)
    : StreamingStoreSourceBase(header, metadata_snapshot_, std::move(query_context_))
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
        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->getBlock();
        auto rows = block.rows();

        /// Block in channel shall always contain full columns
        assert(block.columns() == total_physical_columns_in_schema);

        for (auto pos : column_positions)
        {
            if (pos < total_physical_columns_in_schema)
            {
                /// We can't move the column to columns
                /// since the records can be shared among multiple threads
                columns.push_back(block.getByPosition(pos).column);
            }
            else
            {
                assert(virtual_time_columns_calc[pos - total_physical_columns_in_schema]);
                auto ts = virtual_time_columns_calc[pos - total_physical_columns_in_schema](block.info);
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
