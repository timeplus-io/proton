#include "SchemaNativeReader.h"

#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}
}

namespace DWAL
{
namespace
{
    ALWAYS_INLINE void readData(const DB::ISerialization & serialization, DB::ColumnPtr & column, DB::ReadBuffer & istr, size_t rows)
    {
        DB::ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](DB::ISerialization::SubstreamPath) -> DB::ReadBuffer * { return &istr; };
        settings.avg_value_size_hint = 0;
        settings.position_independent_encoding = false;
        settings.native_format = true;

        DB::ISerialization::DeserializeBinaryBulkStatePtr state;

        serialization.deserializeBinaryBulkStatePrefix(settings, state);
        serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

        if (column->size() != rows)
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data in NativeBlockInputStream. Rows read: {}. Rows expected: {}",
                column->size(),
                rows);
    }

    ALWAYS_INLINE void readDataSkip(const DB::ISerialization & serialization, DB::ReadBuffer & istr, size_t rows)
    {
        DB::ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](DB::ISerialization::SubstreamPath) -> DB::ReadBuffer * { return &istr; };
        settings.avg_value_size_hint = 0;
        settings.position_independent_encoding = false;
        settings.native_format = true;

        DB::ISerialization::DeserializeBinaryBulkStatePtr state;

        serialization.deserializeBinaryBulkStatePrefix(settings, state);
        serialization.deserializeBinaryBulkWithMultipleStreamsSkip(rows, settings, state);
    }
}

SchemaNativeReader::SchemaNativeReader(DB::ReadBuffer & istr_, uint16_t & schema_version_, bool partial_, const SchemaContext & schema_ctx_)
    : istr(istr_), schema_version(schema_version_), partial(partial_), schema_ctx(schema_ctx_)
{
}

DB::Block SchemaNativeReader::read()
{
    if (istr.eof())
    {
        schema_version = NO_SCHEMA;
        return DB::Block{};
    }

    /// Dimensions
    uint16_t columns = 0;
    uint32_t rows = 0;

    readVarUInt(schema_version, istr);
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    const auto & header = schema_ctx.schema_provider->getSchema(schema_version);

    assert(rows > 0);
    assert(columns > 0);
    assert((!partial && columns == header.columns()) || partial);
    /// FIXME, if read schema version doesn't equal to schema version, we will need some data converting
    assert(schema_version == schema_ctx.read_schema_version);

    if (partial)
    {
        /// Clone empty here for easier processing for light ingestion case
        DB::Block res{schema_ctx.schema_provider->getSchema(schema_version).cloneEmpty()};
        readPartial(columns, rows, res);
        return res;
    }

    size_t read_columns = 0;
    const auto & column_positions = schema_ctx.column_positions;
    assert(column_positions.size() <= header.columns());

    DB::Block res;
    res.reserve(column_positions.size());
    for (size_t pos = 0; auto column : header)
    {
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        if (column_positions.empty() || std::find(column_positions.begin(), column_positions.end(), pos) != column_positions.end())
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);

            readData(*serialization, read_column, istr, rows);
            column.column = std::move(read_column);
            res.insert(std::move(column));
            ++read_columns;
        }
        else
            /// Clients like to read some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);

        ++pos;

        if (!column_positions.empty() && column_positions.size() == read_columns)
            /// We have collected we like to collect
            break;
    }
    return res;
}

ALWAYS_INLINE void SchemaNativeReader::readPartial(uint16_t columns, uint32_t rows, DB::Block & res)
{
    if (schema_ctx.column_positions.empty())
        /// Clients didn't pass in any columns positions. It means the want all columns
        readPartialForRequestFull(columns, rows, res);
    else
        readPartialForRequestPartial(columns, rows, res);
}

ALWAYS_INLINE void SchemaNativeReader::readPartialForRequestFull(uint16_t columns, uint32_t rows, DB::Block & res)
{
    /// In file system, we store partial columns, but clients request all columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values

    assert(schema_ctx.column_positions.empty());
    for (uint16_t read_columns = 0; read_columns < columns; ++read_columns)
    {
        uint16_t col_pos = 0;
        readVarUInt(col_pos, istr);

        assert(col_pos < res.columns());

        auto & column = res.getByPosition(col_pos);
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// Data
        DB::ColumnPtr read_column = column.type->createColumn(*serialization);

        readData(*serialization, read_column, istr, rows);
        res.getByPosition(col_pos).column = std::move(read_column);
    }

    for (auto & column : res)
        if (!column.column || column.column->empty())
            column.column = column.type->createColumn()->cloneResized(rows);
}

ALWAYS_INLINE void SchemaNativeReader::readPartialForRequestPartial(uint16_t columns, uint32_t rows, DB::Block & res)
{
    /// In file system, we store partial columns, and clients also request partial columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values

    const auto & column_positions = schema_ctx.column_positions;
    assert(!column_positions.empty());

    uint16_t read_columns = 0;
    uint16_t skipped_columns = 0;
    for (; read_columns + skipped_columns < columns && read_columns < column_positions.size();)
    {
        uint16_t col_pos = 0;
        readVarUInt(col_pos, istr);

        assert(col_pos < res.columns());

        auto & column = res.getByPosition(col_pos);
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        if (std::find(column_positions.begin(), column_positions.end(), col_pos) != column_positions.end())
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);

            readData(*serialization, read_column, istr, rows);
            res.getByPosition(col_pos).column = std::move(read_column);
            ++read_columns;
        }
        else
        {
            /// Clients like to read some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);
            ++skipped_columns;
        }
    }

    DB::Block tmp_res;
    /// Collect requested columns only
    for (auto col_pos : column_positions)
    {
        auto & column = res.getByPosition(col_pos);
        if (!column.column || column.column->empty())
            column.column = column.type->createColumn()->cloneResized(rows);

        tmp_res.insert(std::move(column));
    }
    res.swap(tmp_res);
}
}
