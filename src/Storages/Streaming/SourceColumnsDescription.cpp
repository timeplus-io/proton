#include "SourceColumnsDescription.h"

#include <Core/Block.h>
#include <NativeLog/Record/Record.h>
#include <Storages/StorageSnapshot.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
SourceColumnsDescription::PhysicalColumnPositions &
SourceColumnsDescription::PhysicalColumnPositions::operator=(std::initializer_list<uint16_t> positions_)
{
    positions = std::move(positions_);
    subcolumns.clear();
    return *this;
}

SourceColumnsDescription::PhysicalColumnPositions &
SourceColumnsDescription::PhysicalColumnPositions::operator=(const std::vector<uint16_t> & positions_)
{
    positions = std::move(positions_);
    subcolumns.clear();
    return *this;
}

void SourceColumnsDescription::PhysicalColumnPositions::clear()
{
    positions.clear();
    subcolumns.clear();
}

SourceColumnsDescription::SourceColumnsDescription(const Names & required_column_names, StorageSnapshotPtr storage_snapshot)
    : SourceColumnsDescription(
        storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals().withExtendedObjects(), required_column_names),
        storage_snapshot->getMetadataForQuery()->getSampleBlock(),
        storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects()))
{
}

SourceColumnsDescription::SourceColumnsDescription(const NamesAndTypesList & columns_to_read, const Block & schema, const NamesAndTypesList & all_extended_columns)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    auto column_size = columns_to_read.size();

    positions.reserve(column_size);
    physical_column_positions_to_read.positions.reserve(column_size);
    subcolumns_to_read.reserve(column_size);

    std::vector<uint16_t> read_all_subcolumns_positions;
    read_all_subcolumns_positions.reserve(column_size);

    /// There are three columns
    /// 1) normal physical column position
    /// 2) virtual column position
    /// 3) subcolumn position
    ///
    /// For example, we want to read 7 columns, and the schema have all 5 columns.
    /// @columns_to_read: `virtual_col0, col1, col2, virtual_col3, col4, col3.sub_col5, virtual_col6`
    /// @schema: `col0, col1, col2, col3, col4`
    ///
    /// To Generate:
    /// columns to read from schema: `col1, col2, col4, col3`
    /// @physical_column_positions_to_read: [1, 2, 4, 3]
    /// @virtual_time_columns_calc: [calc_func_for_virtual_col0, cacl_func_for_virtual_col3, cacl_func_for_virtual_col6]
    /// @subcolumns_to_read: [col3.sub_col5]
    ///
    /// @positions: will look like below
    /// [
    ///     Virtual, @pos: pos_in_virtual_time_columns_calc 0,      /// the pos in @virtual_time_columns_calc
    ///     Physical, @pos: pos_in_schema_to_read: 0,               /// the pos in loaded block by @physical_column_positions_to_read
    ///     Physical, @pos: pos_in_schema_to_read: 1,
    ///     Virtual, @pos: pos_in_virtual_time_columns_calc: 1,
    ///     Physical, @pos: pos_in_schema_to_read: 2,
    ///     Sub, @sub_pos: pos_in_subcolumns_to_read: 0,            /// the pos in @subcolumns_to_read
    ///     Virtual, @pos: pos_in_virtual_time_columns_calc: 2,
    /// ]

    auto add_virtual_col = [this](const auto & column, std::function<Int64(const nlog::RecordPtr & record)> func) {
        ReadColumnPosition curr_column_pos(ReadColumnType::VIRTUAL, virtual_col_calcs.size());
        virtual_col_calcs.push_back(std::move(func));
        virtual_col_types.push_back(column.type);
        positions.emplace_back(std::move(curr_column_pos));
    };

    for (const auto & column : columns_to_read)
    {
        if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            add_virtual_col(column, [](const nlog::RecordPtr & record) { return record->getBlock().info.appendTime(); });
        }
        else if (column.name == ProtonConsts::RESERVED_PROCESS_TIME)
        {
            add_virtual_col(column, [](const nlog::RecordPtr &) { return UTCMilliseconds::now(); });
        }
        else if (column.name == ProtonConsts::RESERVED_SHARD)
        {
            add_virtual_col(column, [](const nlog::RecordPtr & record) { return record->getShard(); });
        }
        else if (column.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
        {
            add_virtual_col(column, [](const nlog::RecordPtr & record) { return record->getSN(); });
        }
        else
        {
            /// FIXME, schema version. For non virtual
            /// There are two cases:
            /// 1) physical column (such as `a`)
            /// 2) sub-column (such as `x.y`)
            /// The `name_in_storage` is always main column name (i.e. `a` or `x`)
            auto name_in_storage = column.getNameInStorage();
            auto pos_in_schema = schema.getPositionByName(name_in_storage);
            const auto & column_in_storage = schema.getByName(name_in_storage);

            /// Calculate main column pos
            size_t physical_pos_in_schema_to_read = 0;
            /// We don't need to read duplicate physical columns from schema
            auto physical_pos_iter = std::find(
                physical_column_positions_to_read.positions.begin(), physical_column_positions_to_read.positions.end(), pos_in_schema);
            if (physical_pos_iter == physical_column_positions_to_read.positions.end())
            {
                physical_pos_in_schema_to_read = physical_column_positions_to_read.positions.size();
                physical_column_positions_to_read.positions.emplace_back(pos_in_schema);

                /// json, array(json), tuple(..., json, ...)
                if (column_in_storage.type->hasDynamicSubcolumns())
                {
                    /// We like to read parent json column once if multiple subcolumns of the same json are required
                    /// like `select json.a, json.b from stream`
                    auto find_iter = std::find_if(
                        physical_object_columns_to_read.begin(),
                        physical_object_columns_to_read.end(),
                        [&column](const auto & col_name_type) { return col_name_type.name == column.name; });

                    if (find_iter == physical_object_columns_to_read.end())
                    {
                        if (column.isSubcolumn())
                        {
                            /// When reading a subcolumn of a json like `select json.a from stream`, we will need read the parent `json` column
                            auto name_and_type = all_extended_columns.tryGetByName(name_in_storage);
                            assert(name_and_type);
                            physical_object_columns_to_read.emplace_back(std::move(*name_and_type));
                        }
                        else
                        {
                            /// This column is parent json column, like `select json from stream`, use the name and type directly
                            physical_object_columns_to_read.emplace_back(column);
                        }
                    }
                }
            }
            else
                physical_pos_in_schema_to_read = physical_pos_iter - physical_column_positions_to_read.positions.begin();

            /// For subcolumn, which dependents on the main column
            if (column.isSubcolumn())
            {
                ReadColumnPosition curr_column_pos(ReadColumnType::SUB, physical_pos_in_schema_to_read, subcolumns_to_read.size());

                const auto & subcolumn_name = column.getSubcolumnName();
                NameAndTypePair subcolumn{name_in_storage, subcolumn_name, column_in_storage.type, column.type};
                subcolumns_to_read.emplace_back(subcolumn);

                /// read partial subcolumn of the physical column.
                physical_column_positions_to_read.subcolumns[physical_pos_in_schema_to_read].emplace_back(subcolumn_name);

                positions.emplace_back(std::move(curr_column_pos));
            }
            else
            {
                ReadColumnPosition curr_column_pos(ReadColumnType::PHYSICAL, physical_pos_in_schema_to_read);
                positions.emplace_back(std::move(curr_column_pos));

                read_all_subcolumns_positions.push_back(physical_pos_in_schema_to_read); /// read all physical column.
            }
        }
    }

    /// Clear subcolumns if need to read all subcolumns.
    for (auto pos : read_all_subcolumns_positions)
        physical_column_positions_to_read.subcolumns.erase(pos);

    /// Clients like to read virtual columns only, add `_tp_time`, then we know how many rows
    if (physical_column_positions_to_read.positions.empty())
        physical_column_positions_to_read.positions.emplace_back(schema.getPositionByName(ProtonConsts::RESERVED_EVENT_TIME));
}
}
