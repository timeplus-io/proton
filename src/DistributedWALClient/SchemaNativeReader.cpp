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

/// @param partial_ does the wire format contains partial columns of the schema ? true means only partial columns are stored
/// @param schema_ctx_ schema_ctx_.column_positions, does client request only partial of columns in schema
SchemaNativeReader::SchemaNativeReader(DB::ReadBuffer & istr_, uint16_t & schema_version_, bool partial_, const SchemaContext & schema_ctx_)
    : istr(istr_), schema_version(schema_version_), partial(partial_), schema_ctx(schema_ctx_)
{
}

/// read guarantee that the returned block has the same column order as request if `column_positions` is
/// set in schema context. Then clients don't need sort the block any more. If `column_positions` is not set
/// the returned block has the same column sequence as the schema
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

    /// FIXME, if read schema version doesn't equal to schema version, we will need some data converting
    assert(schema_version == schema_ctx.read_schema_version);

    if (partial)
    {
        /// Clone empty here for easier processing for light ingestion case
        if (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns())
            /// Write partial / request full
            return readPartialForRequestFull(columns, rows, header.cloneEmpty());
        else
            /// Write partial / request partial
            return readPartialForRequestPartial(columns, rows, header);
    }
    else
    {
        assert(columns == header.columns());

        if (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns())
            /// Write full / request full
            return readFullForRequestFull(rows, header.cloneEmpty());
        else
            /// Write full / request partial
            return readFullForRequestPartial(rows, header);
    }
}

ALWAYS_INLINE DB::Block SchemaNativeReader::readFullForRequestFull(uint32_t rows, DB::Block res)
{
    assert (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == res.columns());

    /// We assume the order of the columns serialized has the same column order of the schema
    /// This requests during ingestion, we order the columns according to the schema. Pushing sorting
    /// to ingest stage makes sense since ingestion is more scale and cabe be more concurrent
    for (auto & column : res)
    {
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// Data
        DB::ColumnPtr read_column = column.type->createColumn(*serialization);

        readData(*serialization, read_column, istr, rows);
        column.column = std::move(read_column);
    }

    if (schema_ctx.column_positions.empty())
        return res;

    return sortColumnOrder(std::move(res));
}

ALWAYS_INLINE DB::Block SchemaNativeReader::sortColumnOrder(DB::Block res)
{
    assert (!schema_ctx.column_positions.empty());
    /// column_positions is not empty, we will need sort the columns order according to the request
    DB::Block sorted_block;
    sorted_block.reserve(res.columns());

    for (auto pos : schema_ctx.column_positions)
        sorted_block.insert(std::move(res.getByPosition(pos)));

    return sorted_block;
}

ALWAYS_INLINE DB::Block SchemaNativeReader::readFullForRequestPartial(uint32_t rows, const DB::Block & header)
{
    size_t read_columns = 0;
    const auto & column_positions = schema_ctx.column_positions;
    assert(!column_positions.empty() && column_positions.size() < header.columns());

    /// We want to avoid header.cloneEmpty() here since it copies the columns names for unwanted columns which is slow
    /// We want to avoid complicate sorting or hash table lookup as well
    std::vector<DB::ColumnPtr> all_columns(header.columns(), nullptr);

    for (size_t pos = 0; const auto & column : header)
    {
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// We probably don't need build a hash table for position lookup. Short integer vector lookup is super fast
        if (std::find(column_positions.begin(), column_positions.end(), pos) != column_positions.end())
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);

            readData(*serialization, read_column, istr, rows);
            all_columns[pos] = std::move(read_column);
            ++read_columns;
        }
        else
            /// Clients like to read only some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);

        ++pos;

        if (!column_positions.empty() && column_positions.size() == read_columns)
            /// We have collected we like to collect
            break;
    }

    DB::Block res;
    res.reserve(column_positions.size());

    /// Collect the columns in request order
    for (auto pos : column_positions)
    {
        auto column{header.getByPosition(pos)};
        assert(all_columns[pos]);
        column.column = std::move(all_columns[pos]);
        res.insert(std::move(column));
    }
    return res;
}

ALWAYS_INLINE DB::Block SchemaNativeReader::readPartialForRequestFull(uint16_t columns, uint32_t rows, DB::Block res)
{
    /// In file system, we store partial columns, but clients request all columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values
    assert (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == res.columns());

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

    if (schema_ctx.column_positions.empty())
        return res;

    return sortColumnOrder(std::move(res));
}

ALWAYS_INLINE DB::Block SchemaNativeReader::readPartialForRequestPartial(uint16_t columns, uint32_t rows, const DB::Block & header)
{
    /// In file system, we store partial columns, and clients also request partial columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values
    const auto & column_positions = schema_ctx.column_positions;
    assert (!column_positions.empty() || column_positions.size() < header.columns());

    /// We want to avoid header.cloneEmpty() here since it copies the columns names for unwanted columns which is slow
    /// We want to avoid complicate sorting or hash table lookup as well
    std::vector<DB::ColumnPtr> all_columns(header.columns(), nullptr);

    uint16_t read_columns = 0;
    uint16_t skipped_columns = 0;
    for (; read_columns + skipped_columns < columns && read_columns < column_positions.size();)
    {
        uint16_t col_pos = 0;
        readVarUInt(col_pos, istr);

        assert(col_pos < all_columns.size());

        const auto & column = header.getByPosition(col_pos);
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
            all_columns[col_pos] = std::move(read_column);
            ++read_columns;
        }
        else
        {
            /// Clients like to read some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);
            ++skipped_columns;
        }
    }

    DB::Block res;
    res.reserve(column_positions.size());

    /// Collect requested columns only and according to the request column order
    for (auto col_pos : column_positions)
    {
        auto column{header.getByPosition(col_pos)};
        if (all_columns[col_pos] != nullptr)
            column.column = std::move(all_columns[col_pos]);
        else
            column.column = column.type->createColumn()->cloneResized(rows);

        res.insert(std::move(column));
    }
    return res;
}
}
