#include <Interpreters/Streaming/JoinStreamDescription.h>

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
DataStreamSemanticEx getDataStreamSemantic(StoragePtr storage)
{
    if (!storage)
        return Streaming::DataStreamSemantic::Append;

    return storage->dataStreamSemantic();
}

void JoinStreamDescription::calculateColumnPositions(JoinStrictness strictness)
{
    assert(input_header);

    if (Streaming::isAppendDataStream(data_stream_semantic) || (strictness == JoinStrictness::Any || strictness == JoinStrictness::Asof))
        return;

    if (hasPrimaryKey() || hasDeltaColumn())
        return;

    /// Usually, the formats of column clashes:
    /// 1) for left table, `<column>`
    /// 2) for right table, `<table>.<column>`
    auto calc_column_position = [this](const auto & col_name) -> std::optional<size_t> {
        if (input_header.has(col_name))
            return input_header.getPositionByName(col_name);

        auto col_name_with_table_prefix = table_with_columns.table.getQualifiedNamePrefix() + col_name;
        if (input_header.has(col_name_with_table_prefix))
            return input_header.getPositionByName(col_name_with_table_prefix);

        return {};
    };

    /// If we can find primary key column in the header, compute their column position in the header.
    /// This is a query optimization since we push all of the primary key indexing / retract etc to join phase
    /// instead doing all of these in a separate changelog transform.
    if (auto names = table_with_columns.getColumnNamesWithPrefix(ProtonConsts::PRIMARY_KEY_COLUMN_PREFIX); !names.empty())
    {
        for (const auto & name : names)
        {
            if (auto pos = calc_column_position(name); pos.has_value())
            {
                if (!primary_key_column_positions)
                    primary_key_column_positions.emplace();

                primary_key_column_positions->push_back(*pos);
            }
        }
    }

    /// Calculate version column position
    if (auto names = table_with_columns.getColumnNamesWithPrefix(ProtonConsts::VERSION_COLUMN_PREFIX); !names.empty())
    {
        if (names.size() > 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only support at most one version column, but got: {}", fmt::join(names, ", "));

        version_column_position = calc_column_position(names[0]);
    }

    /// Calculate delta flag column position
    if (table_with_columns.hasColumn(ProtonConsts::RESERVED_DELTA_FLAG))
        delta_column_position = calc_column_position(ProtonConsts::RESERVED_DELTA_FLAG);

    checkValid();
}

const String & JoinStreamDescription::deltaColumnName() const
{
    assert(hasDeltaColumn());

    return input_header.getByPosition(*delta_column_position).name;
}

void JoinStreamDescription::checkValid() const
{
    /// If it is a changelog data stream, we are expecting `delta` column or `primary key + version column`
    /// are there in the input
    if (Streaming::isChangelogDataStream(data_stream_semantic) && (!hasDeltaColumn() && !(hasPrimaryKey() && hasVersionColumn())))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The changelog data stream requires 'delta column' or 'primary key + version column' in the input");
}
}
}
