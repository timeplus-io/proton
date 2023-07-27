#include <Formats/SimpleNativeWriter.h>

#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
void writeData(const ISerialization & serialization, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
}
}

void writeBlock(const Block & block, UInt64 client_revision, WriteBuffer & ostr)
{
    /// Additional information about block
    if (client_revision > 0)
        block.info.write(ostr);

    block.checkNumberOfRows();

    /// Dimensions
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column = block.safeGetByPosition(i);

        /// Name
        writeStringBinary(column.name, ostr);

        /// Type
        String type_name = column.type->getName();
        writeStringBinary(type_name, ostr);

        setVersionToAggregateFunctions(column.type, true, client_revision);

        /// Serialization. Dynamic, if client supports it.
        auto info = column.type->getSerializationInfo(*column.column);
        auto serialization = column.type->getSerialization(*info);

        bool has_custom = info->hasCustomSerialization();
        writeBinary(static_cast<UInt8>(has_custom), ostr);
        if (has_custom)
            info->serialializeKindBinary(ostr);

        /// Data
        if (rows) /// Zero items of data is always represented as zero number of bytes.
            writeData(*serialization, column.column, ostr, 0, 0);
    }
}

void writeChunk(const Chunk & chunk, const Block & header, UInt64 client_revision, WriteBuffer & ostr)
{
    /// TODO: need ?
    // if (chunk.hasChunkInfo())
    // if (chunk.hasChunkContext())

    if (header.columns() != chunk.getNumColumns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write chunk because header has {} columns, but {} columns given.",
            header.columns(),
            chunk.getNumColumns());

    /// Dimensions
    size_t columns = chunk.getNumColumns();
    size_t rows = chunk.getNumRows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    if (rows == 0)
        return;

    for (size_t i = 0; const auto & column : chunk.getColumns())
    {
        auto type = header.getByPosition(i).type;

        setVersionToAggregateFunctions(type, true, client_revision);

        /// Serialization. Dynamic, if client supports it.
        auto info = type->getSerializationInfo(*column);
        auto serialization = type->getSerialization(*info);

        bool has_custom = info->hasCustomSerialization();
        writeBinary(static_cast<UInt8>(has_custom), ostr);
        if (has_custom)
            info->serialializeKindBinary(ostr);

        /// Data
        writeData(*serialization, column, ostr, 0, 0);

        ++i;
    }
}

template <typename DataBlock>
requires(std::is_same_v<DataBlock, Block> || std::is_same_v<DataBlock, Chunk>)
void SimpleNativeWriter<DataBlock>::flush()
{
    ostr.next();
}

template <typename DataBlock>
requires(std::is_same_v<DataBlock, Block> || std::is_same_v<DataBlock, Chunk>)
void SimpleNativeWriter<DataBlock>::write(const DataBlock & data_block)
{
    if constexpr (std::is_same_v<DataBlock, Block>)
        return writeBlock(data_block, client_revision, ostr);
    else if constexpr (std::is_same_v<DataBlock, Chunk>)
        return writeChunk(data_block, header, client_revision, ostr);

    UNREACHABLE();
}

template class SimpleNativeWriter<Block>;
template class SimpleNativeWriter<Chunk>;
}
