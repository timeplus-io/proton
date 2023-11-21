#include <Storages/Streaming/StreamingStoreSourceChannel.h>

#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/Streaming/StreamingStoreSourceMultiplexer.h>

namespace DB
{
StreamingStoreSourceChannel::StreamingStoreSourceChannel(
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
    Block header,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr query_context_,
    Poco::Logger * log_)
    : StreamingStoreSourceBase(
        header,
        storage_snapshot_,
        /*enable_partial_read*/ false,
        std::move(query_context_),
        log_,
        ProcessorID::StreamingStoreSourceChannelID) /// NOLINT(performance-move-const-arg)
    , id(sequence_id++)
    , multiplexer(std::move(multiplexer_))
    , records_queue(1000)
{
    const auto & settings = query_context->getSettingsRef();
    if (settings.record_consume_timeout_ms.value != 0)
        record_consume_timeout_ms = static_cast<Int32>(settings.record_consume_timeout_ms.value);
}

std::atomic<uint32_t> StreamingStoreSourceChannel::sequence_id = 0;

StreamingStoreSourceChannel::~StreamingStoreSourceChannel()
{
    std::lock_guard lock(multiplexer_mutex);
    multiplexer->removeChannel(id);
}

void StreamingStoreSourceChannel::readAndProcess()
{
    nlog::RecordPtrs records;
    auto got_records = records_queue.tryPop(records, record_consume_timeout_ms);
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

        /// Ingore duplicate records, It's possible for re-attach to shared group from independent multiplexer
        if (record->getSN() <= last_sn)
            continue;

        last_sn = record->getSN();

        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->getBlock();
        auto rows = block.rows();

        /// Block in channel shall always contain full columns
        assert(block.columns() == columns_desc.physical_column_positions_to_read.positions.size());

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
                    /// NOTE: The `FilterTransform` will try optimizing filter ConstColumn to always_false or always_true,
                    /// for exmaple: `_tp_sn < 1`, if filter first data _tp_sn is 0, it will be optimized always_true.
                    /// So we can not create a constant column, since the virtual column data isn't constants value in fact.
                    auto virtual_column
                        = columns_desc.virtual_col_types[pos.virtualPosition()]->createColumnConst(rows, ts)->convertToFullColumnIfConst();
                    columns.push_back(std::move(virtual_column));
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

        result_chunks.emplace_back(std::move(columns), rows);
        if (likely(block.info.appendTime() > 0))
        {
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setAppendTime(block.info.appendTime());
            result_chunks.back().setChunkContext(std::move(chunk_ctx));
        }
    }
    iter = result_chunks.begin();
}

void StreamingStoreSourceChannel::add(nlog::RecordPtrs records)
{
    [[maybe_unused]] auto added = records_queue.emplace(std::move(records));
    assert(added);
}

std::pair<String, Int32> StreamingStoreSourceChannel::getStreamShard() const
{
    std::lock_guard lock(multiplexer_mutex);
    return multiplexer->getStreamShard();
}

void StreamingStoreSourceChannel::attachTo(std::shared_ptr<StreamingStoreSourceMultiplexer> new_multiplexer)
{
    std::lock_guard lock(multiplexer_mutex);
    multiplexer = std::move(new_multiplexer);
}

void StreamingStoreSourceChannel::recover(CheckpointContextPtr ckpt_ctx_)
{
    StreamingStoreSourceBase::recover(std::move(ckpt_ctx_));

    if (last_sn >= 0)
    {
        std::lock_guard lock(multiplexer_mutex);
        assert(multiplexer->totalChannels() == 1);
        multiplexer->resetSequenceNumber(last_sn + 1);
        multiplexer->startup();
    }
}

}
