#include "StreamingStoreSourceBase.h"

#include <Common/ProtonCommon.h>
#include <DataTypes/ObjectUtils.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr query_context_)
    : SourceWithProgress(header)
    , storage_snapshot(*storage_snapshot_)
    , query_context(std::move(query_context_))
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
        fillAndConvertObjectsToTuples(columns_list, block, extended_storage_columns, required_object_names, /*no_convert*/ true);

        /// Update object columns if have changes
        auto current_object_columns = *storage_snapshot.object_columns.get();
        if (DB::updateObjectColumns(current_object_columns, columns_list))
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

    /// std::unique_lock lock(result_blocks_mutex);
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

}
