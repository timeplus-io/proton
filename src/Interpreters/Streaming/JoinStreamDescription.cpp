#include <Interpreters/Streaming/JoinStreamDescription.h>

#include <Storages/IStorage.h>
#include <Common/ProtonCommon.h>

namespace DB::Streaming
{
DataStreamSemantic getDataStreamSemantic(StoragePtr storage)
{
    if (!storage)
        return Streaming::DataStreamSemantic::Append;

    return storage->dataStreamSemantic();
}

void JoinStreamDescription::calculateColumnPositions(JoinStrictness strictness)
{
    assert(input_header);

    if (!Streaming::isKeyedDataStream(data_stream_semantic) || (strictness == JoinStrictness::Any || strictness == JoinStrictness::Asof))
        return;

    if (hasPrimaryKey() || hasDeltaColumn())
        return;

    /// If we can find primary key column in the header, compute their column position in the header.
    /// This is a query optimization since we push all of the primary key indexing / retract etc to join phase
    /// instead doing all of these in a separate changelog transform.
    for (size_t pos = 0; const auto & col : input_header)
    {
        if (col.name.starts_with(ProtonConsts::PRIMARY_KEY_COLUMN_PREFIX))
        {
            if (!primary_key_column_positions)
                primary_key_column_positions.emplace();

            primary_key_column_positions->push_back(pos);
        }

        if (col.name.starts_with(ProtonConsts::VERSION_COLUMN_PREFIX))
            version_column_position = pos;

        if (col.name == ProtonConsts::RESERVED_DELTA_FLAG || col.name.ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
            delta_column_position = pos;

        ++pos;
    }

    assertValid();
}

const String & JoinStreamDescription::deltaColumnName() const
{
    assert (hasDeltaColumn());

    return input_header.getByPosition(*delta_column_position).name;
}

void JoinStreamDescription::assertValid() const
{
    /// If it is a keyed data stream, we are expecting `delta` column or `primary key + version column`
    /// are there in the input
    assert(
        !Streaming::isKeyedDataStream(data_stream_semantic)
        || (Streaming::isKeyedDataStream(data_stream_semantic)
            && (hasDeltaColumn() || (hasPrimaryKey() && hasVersionColumn()))));
}
}
