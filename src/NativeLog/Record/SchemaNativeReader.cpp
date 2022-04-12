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

namespace nlog
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
SchemaNativeReader::SchemaNativeReader(DB::ReadBuffer & istr_, bool partial_, uint16_t schema_version_, const SchemaContext & schema_ctx_)
    : istr(istr_), partial(partial_), schema_version(schema_version_), schema_ctx(schema_ctx_)
{
}

/// read guarantee that the returned block has the same column order as request if `column_positions` is
/// set in schema context. Then clients don't need sort the block any more. If `column_positions` is not set
/// the returned block has the same column sequence as the schema
void SchemaNativeReader::read(DB::Block & res)
{
    assert(!res);

    if (istr.eof())
        return;

    /// Dimensions, note the type here for columns and rows have to match what have been
    /// used in SchemaNativeWriter
    uint16_t columns = 0;
    uint32_t rows = 0;

    DB::readIntBinary(columns, istr);
    DB::readIntBinary(rows, istr);

    const auto & header = schema_ctx.schema_provider->getSchema(schema_version);

    assert(rows > 0);
    assert(columns > 0);

    /// FIXME, if read schema version doesn't equal to schema version, we will need some data converting
    assert(schema_version == schema_ctx.read_schema_version || schema_ctx.read_schema_version == nlog::ALL_SCHEMA);

    if (partial)
    {
        /// Clone empty here for easier processing for light ingestion case
        if (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns())
            /// Write partial / request full
            return readPartialForRequestFull(columns, rows, header, res);
        else
            /// Write partial / request partial
            return readPartialForRequestPartial(columns, rows, header, res);
    }
    else
    {
        assert(columns == header.columns());

        if (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns())
            /// Write full / request full
            return readFullForRequestFull(rows, header, res);
        else
            /// Write full / request partial
            return readFullForRequestPartial(rows, header, res);
    }
}

ALWAYS_INLINE void SchemaNativeReader::readFullForRequestFull(uint32_t rows, const DB::Block & header, DB::Block & res)
{
    assert (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns());

    res.reserve(header.columns());
    /// Clone the header
    for (const auto & col: header)
        res.insert(col);

    /// We assume the order of the columns serialized has the same column order of the schema
    /// This requests during ingestion, we order the columns according to the schema. Pushing sorting
    /// to ingest stage makes sense since ingestion is more scale and can be more concurrent
    for (auto & column : res)
    {
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        DB::readIntBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// Data
        DB::ColumnPtr read_column = column.type->createColumn(*serialization);

        readData(*serialization, read_column, istr, rows);
        column.column = std::move(read_column);
    }

    res.sortColumnInplace(schema_ctx.column_positions);
}

ALWAYS_INLINE void SchemaNativeReader::readFullForRequestPartial(uint32_t rows, const DB::Block & header, DB::Block & res)
{
    size_t read_columns = 0;
    const auto & column_positions = schema_ctx.column_positions;
    size_t request_column_num = column_positions.size();
    assert(!column_positions.empty() && request_column_num < header.columns());

    /// We want to avoid header.cloneEmpty() here since it copies the columns names for unwanted columns which is slow
    /// We want to avoid complicate sorting or hash table lookup as well
    /// We need collect the columns in request order
    std::vector<DB::ColumnPtr> request_columns(request_column_num, nullptr);

    for (size_t pos = 0; const auto & column : header)
    {
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        readIntBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// We probably don't need build a hash table for position lookup. Short integer vector lookup is super fast
        auto iter = std::find(column_positions.begin(), column_positions.end(), pos);
        if (iter != column_positions.end())
        {
            auto target_pos = std::distance(column_positions.begin(), iter);

            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);
            readData(*serialization, read_column, istr, rows);
            request_columns[target_pos] = std::move(read_column);
            ++read_columns;
        }
        else
            /// Clients like to read only some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);

        ++pos;

        if (request_column_num == read_columns)
            /// We have collected we like to collect
            break;
    }

    res.reserve(request_column_num);

    for (size_t i = 0; auto & pos : column_positions)
    {
        auto column{header.getByPosition(pos)};
        column.column = std::move(request_columns[i]);
        res.insert(std::move(column));
        ++i;
    }
}

ALWAYS_INLINE void SchemaNativeReader::readPartialForRequestFull(uint16_t columns, uint32_t rows, const DB::Block & header, DB::Block & res)
{
    /// In file system, we store partial columns, but clients request all columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values
    assert (schema_ctx.column_positions.empty() || schema_ctx.column_positions.size() == header.columns());

    res.reserve(header.columns());
    /// Clone the header
    for (const auto & col: header)
        res.insert(col);

    /// Column positions
    std::vector<uint16_t> serialized_column_positions(columns, 0);
    for (uint16_t i = 0; i < columns; ++i)
    {
        DB::readIntBinary(serialized_column_positions[i], istr);
        assert(serialized_column_positions[i] < header.columns());
    }

    for (uint16_t read_columns = 0; read_columns < columns; ++read_columns)
    {
        uint16_t col_pos = serialized_column_positions[read_columns];

        assert(col_pos < res.columns());

        auto & column = res.getByPosition(col_pos);
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        DB::readIntBinary(has_custom, istr);
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

    res.sortColumnInplace(schema_ctx.column_positions);
}

ALWAYS_INLINE void SchemaNativeReader::readPartialForRequestPartial(uint16_t columns, uint32_t rows, const DB::Block & header, DB::Block & res)
{
    /// In file system, we store partial columns, and clients also request partial columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values
    const auto & column_positions = schema_ctx.column_positions;
    auto request_column_size = column_positions.size();
    assert (!column_positions.empty() && request_column_size < header.columns());

    /// We want to avoid header.cloneEmpty() here since it copies the columns names for unwanted columns which is slow
    /// We want to avoid complicate sorting or hash table lookup as well
    /// Collect requested columns only and according to the request column order
    std::vector<DB::ColumnPtr> request_columns(request_column_size, nullptr);

    /// Column positions
    std::vector<uint16_t> serialized_column_positions(columns, 0);
    for (uint16_t i = 0; i < columns; ++i)
    {
        DB::readIntBinary(serialized_column_positions[i], istr);
        assert(serialized_column_positions[i] < header.columns());
    }

    for (uint16_t read_columns = 0, skipped_columns = 0; read_columns + skipped_columns < columns && read_columns < request_column_size;)
    {
        auto col_pos = serialized_column_positions[read_columns + skipped_columns];

        const auto & column = header.getByPosition(col_pos);
        auto info = column.type->createSerializationInfo({});

        uint8_t has_custom;
        DB::readIntBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        auto iter = std::find(column_positions.begin(), column_positions.end(), col_pos);
        if (iter != column_positions.end())
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);

            auto target_pos = std::distance(column_positions.begin(), iter);

            readData(*serialization, read_column, istr, rows);
            request_columns[target_pos] = std::move(read_column);
            ++read_columns;
        }
        else
        {
            /// Clients like to read some columns, skip unwanted columns
            readDataSkip(*serialization, istr, rows);
            ++skipped_columns;
        }
    }

    res.reserve(request_column_size);

    for (size_t i = 0; auto pos : column_positions)
    {
        auto column{header.getByPosition(pos)};
        if (request_columns[i] != nullptr)
            column.column = std::move(request_columns[i]);
        else
            column.column = column.type->createColumn()->cloneResized(rows);

        res.insert(std::move(column));
        ++i;
    }
}
}
