#include <Formats/SimpleNativeReader.h>

#include <Core/LightChunk.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Processors/Chunk.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_INDEX;
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_ALL_DATA;
}


namespace
{
void readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeBlockInputStream. Rows read: {}. Rows expected: {}",
            column->size(),
            rows);
}

void readColumns(Columns & res, const Block & header, UInt64 server_revision, ReadBuffer & istr)
{
    assert(res.empty());

    if (istr.eof())
        return;

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    if (rows == 0)
        return;

    res.reserve(columns);
    for (size_t i = 0; i < columns; ++i)
    {
        auto type = header.getByPosition(i).type;

        setVersionToAggregateFunctions(type, true, server_revision);

        auto info = type->createSerializationInfo({});

        UInt8 has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        SerializationPtr serialization = type->getSerialization(*info);

        /// Data
        ColumnPtr read_column = type->createColumn(*serialization);

        readData(*serialization, read_column, istr, rows, 0);

        res.emplace_back(std::move(read_column));
    }
}
}

Block readBlock(UInt64 server_revision, ReadBuffer & istr)
{
    Block res;

    if (istr.eof())
        return res;

    /// Additional information about the block.
    if (server_revision > 0)
        res.info.read(istr);

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;

        /// Name
        readBinary(column.name, istr);

        /// Type
        String type_name;
        readStringBinary(type_name, istr);
        column.type = data_type_factory.get(type_name);

        setVersionToAggregateFunctions(column.type, true, server_revision);

        auto info = column.type->createSerializationInfo({});

        UInt8 has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        SerializationPtr serialization = column.type->getSerialization(*info);

        /// Data
        ColumnPtr read_column = column.type->createColumn(*serialization);

        if (rows) /// If no rows, nothing to read.
            readData(*serialization, read_column, istr, rows, 0);

        column.column = std::move(read_column);

        res.insert(std::move(column));
    }

    return res;
}

Chunk readChunk(const Block & header, UInt64 server_revision, ReadBuffer & istr)
{
    Columns columns;

    readColumns(columns, header, server_revision, istr);

    auto rows = columns.empty() ? 0 : columns[0]->size();
    return Chunk{std::move(columns), rows};
}

LightChunk readLightChunk(const Block & header, UInt64 server_revision, ReadBuffer & istr)
{
    LightChunk res;
    readColumns(res.data, header, server_revision, istr);
    return res;
}

LightChunkWithTimestamp readLightChunkWithTimestamp(const Block & header, UInt64 server_revision, ReadBuffer & istr)
{
    LightChunkWithTimestamp res;
    readIntBinary(res.min_timestamp, istr);
    readIntBinary(res.max_timestamp, istr);
    readColumns(res.data, header, server_revision, istr);
    return res;
}

template <typename DataBlock>
DataBlock SimpleNativeReader<DataBlock>::read()
{
    if constexpr (std::is_same_v<DataBlock, Block>)
        return readBlock(server_revision, istr);
    else if constexpr (std::is_same_v<DataBlock, Chunk>)
        return readChunk(header, server_revision, istr);
    else if constexpr (std::is_same_v<DataBlock, LightChunk>)
        return readLightChunk(header, server_revision, istr);
    else if constexpr (std::is_same_v<DataBlock, LightChunkWithTimestamp>)
        return readLightChunkWithTimestamp(header, server_revision, istr);

    UNREACHABLE();
}

template class SimpleNativeReader<Block>;
template class SimpleNativeReader<Chunk>;
template class SimpleNativeReader<LightChunk>;
template class SimpleNativeReader<LightChunkWithTimestamp>;
}
