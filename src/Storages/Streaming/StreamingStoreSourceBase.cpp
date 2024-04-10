#include "StreamingStoreSourceBase.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/ObjectUtils.h>
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

StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr query_context_, Poco::Logger * logger_, ProcessorID pid_)
    : Streaming::ISource(header, true, pid_)
    , storage_snapshot(
          std::make_shared<StorageSnapshot>(*storage_snapshot_)) /// We like to make a copy of it since we will mutate the snapshot
    , query_context(std::move(query_context_))
    , logger(logger_)
    , header_chunk(header.getColumns(), 0)
    , columns_desc(header.getNames(), storage_snapshot)
{
    /// Reset current object description and update current storage snapshot for streaming source
    if (!columns_desc.physical_object_columns_to_read.empty())
        storage_snapshot->object_columns.set(std::make_unique<ColumnsDescription>(storage_snapshot->object_columns.get()->getByNames(
            GetColumnsOptions::AllPhysical, columns_desc.physical_object_columns_to_read.getNames())));

    iter = result_chunks_with_sns.begin();
}

ColumnPtr
StreamingStoreSourceBase::getSubcolumnFromBlock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const
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
            Block subcolumn_block({ColumnWithTypeAndName{std::move(subcolumn), std::move(subcolumn_type), subcolumn_pair.name}}); /// NOLINT(performance-move-const-arg)
            ExpressionActions convert_act(ActionsDAG::makeConvertingActions(
                subcolumn_block.getColumnsWithTypeAndName(),
                {ColumnWithTypeAndName{subcolumn_pair.type->createColumn(), subcolumn_pair.type, subcolumn_pair.name}},
                ActionsDAG::MatchColumnsMode::Position));
            convert_act.execute(subcolumn_block, block.rows());
            return subcolumn_block.getByPosition(0).column;
        }
        else if (storage_snapshot->object_columns.get()->hasSubcolumn(subcolumn_pair.name))
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

void StreamingStoreSourceBase::fillAndUpdateObjectsIfNecessary(Block & block)
{
    /// Note
    /// 1. JSON column is appended to streaming data store as it is.
    /// 2. When committing JSON column to historical data store, we will convert it to extended object (tuple(...))
    /// For now, requested columns which have `json` storage type, will have `tuple(...)` in header
    /// we will need convert json column read from streaming store to tuple(...)

    if (!hasDynamicSubcolumns())
        return;

    try
    {
        convertDynamicColumnsToTuples(block, storage_snapshot);

        /// Update json columns if have changes
        /// auto current_object_columns = *storage_snapshot->object_columns.get();
        /// if (updateObjectColumns(current_object_columns, storage_snapshot->metadata->getColumns(), block.getNamesAndTypesList()))
        ///    storage_snapshot->object_columns.set(std::make_unique<ColumnsDescription>(std::move(current_object_columns)));

        /// We will need convert json named tuple if json schema gets changed.
        /// For example, json column in `storage_snapshot` initially has {"k1":"..."} (header has tuple(k1)), but later, json data
        /// gets changed to {"k1":"...", "k2":"..."}, we actually like to convert {"k1":"...", "k2":"..."} to {"k1":"..."}.
        /// The conversion may be the other way: {"k1":"..."} -> {"k1":"...", "k2":"..."}
        /// The conversion can be just type conversion like from `"k1": int` to "k1": string`.
        /// The conversion is recursive if a json object is nested in array, tuple, or has nested dynamic keys
        /// The conversion means once we have a schema version : we stick to this version until the conversion failed (schema in-compatible)
        /// We can't upgrade the schema to have a super set of them (all json subcolumns etc) because the pipeline header
        /// is fixed and can't be changed when query is running
        /// Due to this limitation, we have several consequences which we shall fix:
        /// 1) if a query is running before any json data get ingested, the json will have {"_dummy":int8})
        ///    schema and won't change in the lifetime of the streaming query.
        /// 2) if a query initially has a subset keys of a json and then more json keys get ingested, the final result
        ///    will be a subset of json
        /// One workaround for this is initially ingest a super set of JSON which contains all possible keys or re-run the streaming query
        /// as more data set is ingested

        /// We will need to copy out the object columns and do the conversion on the copy
        Block obj_block;
        obj_block.reserve(columns_desc.physical_object_columns_to_read.size());
        for (const auto & col_name_type : columns_desc.physical_object_columns_to_read)
            obj_block.insert(block.getByName(col_name_type.name));

        performRequiredConversions(obj_block, columns_desc.physical_object_columns_to_read, query_context);

        /// After conversion, move it back to original block
        for (auto & col_name_type : obj_block)
        {
            auto & col = block.getByName(col_name_type.name);
            col.column = std::move(col_name_type.column);
            col.type = std::move(col_name_type.type);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("json data has incompatible schema changes");
        throw;
    }
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
            /// Act as a heart beat
            return header_chunk.clone();

        /// result_blocks is not empty, fallthrough
    }

    setLastProcessedSN(iter->second);
    return std::move((iter++)->first);
}

/// 1) Generate a checkpoint barrier
/// 2) Checkpoint the sequence number just before the barrier
Chunk StreamingStoreSourceBase::doCheckpoint(CheckpointContextPtr current_ckpt_ctx)
{
    /// Prepare checkpoint barrier chunk
    auto result = header_chunk.clone();
    result.setCheckpointContext(current_ckpt_ctx);

    /// Commit the checkpoint : shard
    auto stream_shard = getStreamShard();
    auto processor_id = static_cast<UInt32>(pid);

    /// FIXME : async checkpointing
    current_ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), current_ckpt_ctx, [&](WriteBuffer & wb) {
        writeIntBinary(processor_id, wb);
        writeStringBinary(stream_shard.first, wb);
        writeIntBinary(stream_shard.second, wb);
        writeIntBinary(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint sn={}", lastProcessedSN());

    /// FIXME, if commit failed ?
    /// Propagate checkpoint barriers
    return result;
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
        setLastProcessedSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered last_sn={}", lastProcessedSN());
}

}
