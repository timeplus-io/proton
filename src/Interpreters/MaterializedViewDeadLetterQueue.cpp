#include <Interpreters/MaterializedViewDeadLetterQueue.h>

#include <Columns/ColumnArray.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

NamesAndTypesList MaterializedViewDLQElement::getNamesAndTypes()
{
    return {
        {"node_id", std::make_shared<DataTypeUInt64>()},
        {"database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"view_name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"error", std::make_shared<DataTypeString>()},
        /// `messages` is effectively a nested column as
        /// messages nested(
        ///   source_name low_cardinality(string),
        ///   source_description string,
        ///   sn_range tuple(start uint64, end uint64)
        ///   raw_data array(string)
        /// )
        {"messages.source_name",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))},
        {"messages.source_desc", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"messages.sn_range.start", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"messages.sn_range.end", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"messages.raw_data", std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()))},
        {"_tp_time", std::make_shared<DataTypeDateTime64>(3)},
        {"_tp_sn", std::make_shared<DataTypeInt64>()},
    };
}

void MaterializedViewDLQElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx{0};

    columns[column_idx++]->insert(node_id);
    columns[column_idx++]->insert(database);
    columns[column_idx++]->insert(view_name);
    columns[column_idx++]->insert(uuid);
    columns[column_idx++]->insert(error);

    {
        auto & column_source_name = typeid_cast<ColumnArray &>(*columns[column_idx++]);
        auto & column_source_desc = typeid_cast<ColumnArray &>(*columns[column_idx++]);
        auto & column_sn_range_start = typeid_cast<ColumnArray &>(*columns[column_idx++]);
        auto & column_sn_range_end = typeid_cast<ColumnArray &>(*columns[column_idx++]);
        auto & column_raw_data = typeid_cast<ColumnArray &>(*columns[column_idx++]);

        for (const auto & letter : dead_letters)
        {
            column_source_name.getData().insertData(letter.source_name.data(), letter.source_name.size());
            column_source_desc.getData().insertData(letter.source_description.data(), letter.source_description.size());

            column_sn_range_start.getData().insert(letter.sn_range.first);
            column_sn_range_end.getData().insert(letter.sn_range.second);

            {
                auto & col = typeid_cast<ColumnArray &>(column_raw_data.getData());
                for (const auto & msg : letter.messages)
                    col.getData().insertData(msg.data(), msg.size());
                auto & offsets = col.getOffsets();
                offsets.push_back(offsets.back() + letter.messages.size());
            }
        }

        for (auto * col : std::array<ColumnArray *, 5>{
                 &column_source_name, &column_source_desc, &column_sn_range_start, &column_sn_range_end, &column_raw_data})
        {
            auto & offsets = col->getOffsets();
            offsets.push_back(offsets.back() + dead_letters.size());
        }
    }

    columns[column_idx++]->insert(_tp_time);
}

MaterializedViewDeadLetterQueue::MaterializedViewDeadLetterQueue(
    ContextPtr context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : SystemLog<MaterializedViewDLQElement>(context_, database_name_, table_name_, storage_def_, flush_interval_milliseconds_)
{
    is_force_prepare_tables = true;
}
}
