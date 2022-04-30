#include "StreamingStoreSourceBase.h"

#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include <DataTypes/ObjectUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr query_context_)
    : SourceWithProgress(header)
    , storage_snapshot(storage_snapshot_->storage, storage_snapshot_->metadata, *(storage_snapshot_->object_columns.get()))
    , query_context(std::move(query_context_))
    , header_chunk(header.getColumns(), 0)
    , column_positions(header.columns(), 0)
    , virtual_time_columns_calc(header.columns(), nullptr)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    calculateColumnPositions(header, storage_snapshot_->getMetadataForQuery()->getSampleBlock());

    /// Init current object descriprion and update current storage snapshot for streaming source
    if (!physical_object_column_names_to_read.empty())
        storage_snapshot.object_columns.set(std::make_unique<ColumnsDescription>(
            storage_snapshot.object_columns.get()->getByNames(GetColumnsOptions::AllPhysical, physical_object_column_names_to_read)));

    iter = result_chunks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

ColumnPtr StreamingStoreSourceBase::getSubcolumnFromblock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const
{
    assert(subcolumn_pair.isSubcolumn());

    const auto & parent = block.getByPosition(parent_column_pos);
    if (isObject(parent.type))
    {
        const auto & object = assert_cast<const ColumnObject &>(*parent.column);
        if (const auto * node = object.getSubcolumns().findBestMatch(PathInData{subcolumn_pair.getSubcolumnName()}))
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

        fillAndConvertObjectsToTuples(columns_list, block, extended_storage_columns, /*no_convert*/ true);

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

void StreamingStoreSourceBase::calculateColumnPositions(const Block & header, const Block & schema)
{
    auto header_size = header.columns();
    virtual_columns_pos_begin = schema.columns();
    subcolumns_pos_begin = schema.columns() + header_size;

    physical_column_positions_to_read.reserve(header_size);
    physical_object_column_names_to_read.reserve(header_size);
    subcolumns_to_read.reserve(header_size);
    const auto & columns = storage_snapshot.getColumnsByNames(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), header.getNames());

    /// Calculate column positions in schema
    /// We assign normal physical column position, virtual column position and subcolumn position by using the following algorithm
    /// 1) physical columns pos = 'pos in schema'
    /// 2) virtual columns pos = 'pos in header' + schema.columns()
    /// 3) subcolumns pos = 'parent column pos in schema' + schema.columns() + header.columns()
    ///
    /// For example, header contain 7 columns, schema have all 5 columns.
    /// header: `virtual_col0, col1, col2, virtual_col3, col4, col3.sub_col5, virtual_col6`
    /// schema: `col0, col1, col2, col3, col4`
    ///
    ///  Positions 1, 2, 4 in header are physical columns,
    ///  Positions 0, 3, 6 in header are virtual columns,
    ///  Positions 5 in header is subcolumn,
    ///
    ///  virtual_columns_pos_begin is 5
    ///  subcolumns_pos_begin is 12 <- (5 + 7)
    ///
    /// column positions will look like below
    /// [0 + 5, 1, 2, 3 + 5, 4, 3 + 12, 6 + 5]
    ///
    /// 'virtual_time_columns_calc' vector will look like
    /// [lambda1, nullptr, nullptr, lambda2, nullptr, nullptr, lambda3]
    ///
    /// 'subcolumn_names_to_read' vector will look like
    /// [sub_col5]
    ///
    /// We calculate these column positions and lambda vector for simplify the logic and
    /// fast processing in readAndProcess since we don't need index by column name any more
    ///
    /// Summary of position ranges:
    /// [0, ..., virtual_columns_pos_begin)                                   <= physical columns pos range
    /// [virtual_columns_pos_begin, ..., subcolumns_pos_begin)                <= virtual columns pos range
    /// [subcolumns_pos_begin, ..., subcolumns_pos_begin + header.columns())  <= subcolumns pos range

    for (size_t pos = 0; const auto & column : columns)
    {
        if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo & bi) { return bi.append_time; };
            column_positions[pos] = pos + virtual_columns_pos_begin;
            /// We are assuming all virtual timestamp columns have the same data type
            virtual_col_type = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_PROCESS_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo &) { return UTCMilliseconds::now(); };
            column_positions[pos] = pos + virtual_columns_pos_begin;
            virtual_col_type = column.type;
        }
        else
        {
            /// FIXME, schema version. For non virtual
            auto name_in_storage = column.getNameInStorage();
            auto column_pos = schema.getPositionByName(name_in_storage);
            const auto & column_in_storage = schema.getByName(name_in_storage);
            if (column.isSubcolumn())
            {
                column_positions[pos] = column_pos + subcolumns_pos_begin;
                subcolumns_to_read.emplace_back(name_in_storage, column.getSubcolumnName(), column_in_storage.type, column.type);
            }
            else
                column_positions[pos] = column_pos;

            if (physical_column_positions_to_read.end()
                == std::find(physical_column_positions_to_read.begin(), physical_column_positions_to_read.end(), column_pos))
            {
                physical_column_positions_to_read.push_back(column_pos);

                if (isObject(column_in_storage.type))
                    physical_object_column_names_to_read.push_back(name_in_storage);
            }
        }

        ++pos;
    }

    /// Clients like to read virtual columns only, add `_tp_time`, then we know how many rows
    if (physical_column_positions_to_read.empty())
        physical_column_positions_to_read.push_back(schema.getPositionByName(ProtonConsts::RESERVED_EVENT_TIME));
}
}
