#include <Storages/Stream/StreamingStoreSourceBase.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/StorageSnapshot.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace
{
cluster::SourceColumnsDescription createSourceColumnsDescription(const Names & required_column_names, StorageSnapshotPtr storage_snapshot)
{
    return cluster::SourceColumnsDescription(
        storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals().withExtendedObjects(), required_column_names),
        storage_snapshot->getMetadataForQuery()->getSampleBlock(),
        storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects()));
}
}

StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr query_context_, LoggerPtr logger_, ProcessorID pid_)
    : Streaming::ISource(header, true, logger_, pid_)
    , storage_snapshot(
          std::make_shared<StorageSnapshot>(*storage_snapshot_)) /// We like to make a copy of it since we will mutate the snapshot
    , query_context(std::move(query_context_))
    , header_chunk(header.getColumns(), 0)
    , columns_desc(createSourceColumnsDescription(header.getNames(), storage_snapshot))
{
    iter = result_chunks_with_sns.begin();
}

ColumnPtr
StreamingStoreSourceBase::getSubcolumnFromBlock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const
{
    assert(subcolumn_pair.isSubcolumn());

    const auto & parent = block.getByPosition(parent_column_pos);
    return parent.type->getSubcolumn(subcolumn_pair.getSubcolumnName(), parent.column);
}

void StreamingStoreSourceBase::process(cluster::SchemaRecordPtrs & records)
{
    chassert(!records.empty());

    result_chunks_with_sns.clear();
    result_chunks_with_sns.reserve(records.size());

    for (const auto & record : records)
    {
        if (record->empty())
            continue;

        if (dedupBlock(record))
            continue;

        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->getBlock();
        auto rows = block.rows();

        assert(columns_desc.positions.size() >= block.columns());

        for (const auto & pos : columns_desc.positions)
        {
            switch (pos.type())
            {
                case cluster::SourceColumnsDescription::ReadColumnType::Physical:
                {
                    columns.push_back(block.getByPosition(pos.physicalPosition()).column);
                    break;
                }
                case cluster::SourceColumnsDescription::ReadColumnType::Virtual:
                {
                    /// The current column to return is a virtual column which needs be calculated lively
                    assert(columns_desc.virtual_col_calcs[pos.virtualPosition()]);
                    auto ts = columns_desc.virtual_col_calcs[pos.virtualPosition()](record);
                    /// NOTE: The `FilterTransform` will try optimizing filter ConstColumn to always_false or always_true,
                    /// for example: `_tp_sn < 1`, if filter first data _tp_sn is 0, it will be optimized always_true.
                    /// So we can not create a constant column, since the virtual column data isn't constants value in fact.
                    auto virtual_column
                        = columns_desc.virtual_col_types[pos.virtualPosition()]->createColumnConst(rows, ts)->convertToFullColumnIfConst();
                    columns.push_back(std::move(virtual_column));
                    break;
                }
                case cluster::SourceColumnsDescription::ReadColumnType::Sub:
                {
                    columns.push_back(
                        getSubcolumnFromBlock(block, pos.parentPosition(), columns_desc.subcolumns_to_read[pos.subPosition()]));
                    break;
                }
            }
        }

        result_chunks_with_sns.emplace_back(Chunk{std::move(columns), rows}, record->getSN());
        if (auto append_ts = record->getAppendTime(); append_ts > 0)
        {
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setAppendTime(append_ts);
            result_chunks_with_sns.back().first.setChunkContext(std::move(chunk_ctx));
        }
    }
    iter = result_chunks_with_sns.begin();

    if (auto append_ts = records.back()->getAppendTime(); append_ts > 0)
        setLastProcessedRecordTimestamp(append_ts);
}

Chunk StreamingStoreSourceBase::generate()
{
    if (isCancelled())
        return {};

    if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
        {
            /// Act as a heart beat
            auto heartbeat_chunk = header_chunk.clone();

            /// If there are gaps due to empty entries/records, \processed_sn also needs to be advanced
            if (auto fetched_sn = lastFetchedSN(); fetched_sn > lastProcessedSN())
            {
                setLastProcessedSN(fetched_sn);

                /// Always set the sequence number to the chunk context for remote read
                if (query_context->getSettingsRef().remote_fetch.value)
                    heartbeat_chunk.setSN(fetched_sn);
            }
            return heartbeat_chunk;
        }

        /// result_blocks is not empty, fallthrough
    }

    setLastProcessedSN(iter->second);

    /// Always set the sequence number to the chunk context for remote read
    if (query_context->getSettingsRef().remote_fetch.value)
        iter->first.setSN(iter->second);

    return std::move((iter++)->first);
}

void StreamingStoreSourceBase::doCheckpoint(CheckpointContextPtr current_ckpt_ctx)
{
    auto stream_shard = getStreamShard();
    auto processor_id = static_cast<UInt32>(pid);

    current_ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), current_ckpt_ctx, [&](WriteBuffer & wb) {
        writeIntBinary(processor_id, wb);
        writeStringBinary(stream_shard.first, wb);
        writeIntBinary(stream_shard.second, wb);
        writeIntBinary(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint sn={}", lastProcessedSN());
}

void StreamingStoreSourceBase::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType /*version*/, ReadBuffer & rb) {
        UInt32 recovered_pid = 0;
        readIntBinary(recovered_pid, rb);
        if (recovered_pid != static_cast<UInt32>(pid))
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched processor ID. Recovered={}, new={}",
                recovered_pid,
                static_cast<UInt32>(pid));

        std::pair<String, Int32> recovered_stream_shard;
        readStringBinary(recovered_stream_shard.first, rb);
        readIntBinary(recovered_stream_shard.second, rb);

        auto current_stream_shard = getStreamShard();
        if (recovered_stream_shard != current_stream_shard)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched stream shard. recovered={}-{}, current={}-{}",
                recovered_stream_shard.first,
                recovered_stream_shard.second,
                current_stream_shard.first,
                current_stream_shard.second);

        Int64 recovered_last_sn = 0;
        readIntBinary(recovered_last_sn, rb);
        setLastCheckpointSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered last_sn={}", lastCheckpointSN());
}

bool StreamingStoreSourceBase::dedupBlock(const cluster::SchemaRecordPtr & record)
{
    if (!idempotent_keys)
        return false;

    return !idempotent_keys->add(record->getSN(), record->idempotentKey(), /*deduped_log_prefix=*/"Skipping duplicate block processing");
}
}
