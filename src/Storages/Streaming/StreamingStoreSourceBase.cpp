#include "StreamingStoreSourceBase.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/ObjectUtils.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int RECOVER_CHECKPOINT_FAILED;
}

StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr query_context_, Poco::Logger * log_, ProcessorID pid_)
    : SourceWithProgress(header, pid_)
    , storage_snapshot(*storage_snapshot_)
    , query_context(std::move(query_context_))
    , log(log_)
    , header_chunk(header.getColumns(), 0)
    , columns_desc(
          storage_snapshot.getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals(), header.getNames()),
          storage_snapshot.getMetadataForQuery()->getSampleBlock())
    , required_object_names(getNamesOfObjectColumns(header.getNamesAndTypesList()))
{
    /// Init current object description and update current storage snapshot for streaming source
    if (!columns_desc.physical_object_column_names_to_read.empty())
        storage_snapshot.object_columns.set(std::make_unique<ColumnsDescription>(storage_snapshot.object_columns.get()->getByNames(
            GetColumnsOptions::AllPhysical, columns_desc.physical_object_column_names_to_read)));

    iter = result_chunks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

ColumnPtr
StreamingStoreSourceBase::getSubcolumnFromblock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const
{
    assert(subcolumn_pair.isSubcolumn());

    const auto & parent = block.getByPosition(parent_column_pos);
    if (isObject(parent.type))
    {
        const auto & object = assert_cast<const ColumnObject &>(*parent.column);
        if (const auto * node = object.getSubcolumns().findExact(PathInData{subcolumn_pair.getSubcolumnName()}))
        {
            auto [subcolumn, subcolumn_type] = createSubcolumnFromNode(*node);
            if (subcolumn_type->equals(*subcolumn_pair.type))
                return subcolumn;

            /// Convert subcolumn if the subcolumn type of dynamic object may be dismatched with header.
            /// FIXME: Cache the ExpressionAction
            Block subcolumn_block({ColumnWithTypeAndName{std::move(subcolumn), std::move(subcolumn_type), subcolumn_pair.name}});
            ExpressionActions convert_act(ActionsDAG::makeConvertingActions(
                subcolumn_block.getColumnsWithTypeAndName(),
                {ColumnWithTypeAndName{subcolumn_pair.type->createColumn(), subcolumn_pair.type, subcolumn_pair.name}},
                ActionsDAG::MatchColumnsMode::Position));
            convert_act.execute(subcolumn_block, block.rows());
            return subcolumn_block.getByPosition(0).column;
        }
        else if (storage_snapshot.object_columns.get()->hasSubcolumn(subcolumn_pair.name))
        {
            /// we return default value if the object has this subcolumn but current block doesn't exist
            return subcolumn_pair.type->createColumn()->cloneResized(block.rows());
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject {}", subcolumn_pair.getSubcolumnName(), parent.name);
    }
    else
        return parent.type->getSubcolumn(subcolumn_pair.getSubcolumnName(), parent.column);
}

void StreamingStoreSourceBase::fillAndUpdateObjects(Block & block)
{
    try
    {
        auto columns_list = storage_snapshot.metadata->getColumns().getAllPhysical().filter(block.getNames());
        auto extended_storage_columns
            = storage_snapshot.getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects());

        /// Fill missing elems for objects in block (only filled those @required_object_names)
        fillAndConvertObjectsToTuples(columns_list, block, extended_storage_columns, required_object_names);

        /// Update object columns if have changes
        auto current_object_columns = *storage_snapshot.object_columns.get();
        if (updateObjectColumns(current_object_columns, storage_snapshot.metadata->getColumns(), columns_list))
            storage_snapshot.object_columns.set(std::make_unique<ColumnsDescription>(std::move(current_object_columns)));
    }
    catch (Exception & e)
    {
        e.addMessage("Failed to fill and update objects.");
        throw;
    }
}

Chunk StreamingStoreSourceBase::generate()
{
    if (isCancelled())
        return {};

    if (auto * current = ckpt_ctx.exchange(nullptr, std::memory_order_relaxed); current)
        return doCheckpoint(CheckpointContextPtr{current});

    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks.empty() || iter == result_chunks.end())
        {
            /// Act as a heart beat and flush
            last_flush_ms = MonotonicMilliseconds::now();
            return header_chunk.clone();
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return header_chunk.clone();
    }

    return std::move(*iter++);
}

/// It basically initiate a checkpoint
/// Since the checkpoint method is called in a different thread (CheckpointCoordinator)
/// We nee make sure it is thread safe
void StreamingStoreSourceBase::checkpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// We assume the previous ckpt is already done
    /// Use std::atomic<std::shared_ptr<CheckpointContext>>
    assert(!ckpt_ctx.load(std::memory_order_relaxed));
    ckpt_ctx = new CheckpointContext(*ckpt_ctx_);
}

/// 1) Generate a checkpoint barrier
/// 2) Checkpoint the sequence number just before the barrier
Chunk StreamingStoreSourceBase::doCheckpoint(CheckpointContextPtr current_ckpt_ctx)
{
    assert(current_ckpt_ctx->epoch > last_epoch);

    /// Prepare checkpoint barrier chunk
    auto result = header_chunk.clone();
    auto chunk_ctx = std::make_shared<ChunkContext>();
    chunk_ctx->setCheckpointContext(current_ckpt_ctx);
    result.setChunkContext(std::move(chunk_ctx));

    /// Commit the checkpoint : shard
    auto stream_shard = getStreamShard();
    auto processor_id = static_cast<UInt32>(pid);

    /// FIXME : async checkpointing
    current_ckpt_ctx->coordinator->checkpoint(getVersion(), logic_pid, current_ckpt_ctx, [&](WriteBuffer & wb) {
        writeIntBinary(processor_id, wb);
        writeStringBinary(stream_shard.first, wb);
        writeIntBinary(stream_shard.second, wb);
        writeIntBinary(last_sn, wb);
    });

    /// FIXME, if commit failed ?
    /// Propagate checkpoint barriers
    return result;
}

void StreamingStoreSourceBase::recover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(logic_pid, ckpt_ctx_, [&](VersionType /*version*/, ReadBuffer & rb) {
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

        readIntBinary(last_sn, rb);
    });

    LOG_INFO(log, "Recovered last_sn={}", last_sn);
}

}
