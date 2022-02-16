#include "SchemaNativeReader.h"

#include <Core/Defines.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

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

SchemaNativeReader::SchemaNativeReader(DB::ReadBuffer & istr_, uint16_t & schema_version_, const SchemaProvider & schema_)
    : istr(istr_), schema_version(schema_version_), schema(schema_)
{
}

DB::Block SchemaNativeReader::read()
{
    DB::Block res;

    if (istr.eof())
    {
        schema_version = NO_SCHEMA;
        return res;
    }

    /// Dimensions
    uint16_t columns = 0;
    uint32_t rows = 0;

    readVarUInt(schema_version, istr);
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    const auto & header = schema.getSchema(schema_version);
    assert(rows > 0);
    assert(columns > 0);
    assert(columns == header.columns());

    for (auto column : header)
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

        res.insert(std::move(column));
    }

    //    if (rows && header)
    //    {
    //        /// Allow to skip columns. Fill them with default values.
    //        Block tmp_res;
    //
    //        for (auto & col : header)
    //        {
    //            if (res.has(col.name))
    //                tmp_res.insert(res.getByName(col.name));
    //            else
    //                tmp_res.insert({col.type->createColumn()->cloneResized(rows), col.type, col.name});
    //        }
    //
    //        res.swap(tmp_res);
    //    }

    return res;
}

DB::Block SchemaNativeReader::read(const std::vector<size_t> & column_positions)
{
    DB::Block res;

    if (istr.eof())
    {
        schema_version = NO_SCHEMA;
        return res;
    }

    /// Dimensions
    uint16_t columns = 0;
    uint32_t rows = 0;

    readVarUInt(schema_version, istr);
    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    const auto & header = schema.getSchema(schema_version);
    assert(rows > 0);
    assert(columns > 0);
    assert(columns == header.columns());

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
        }
        else
            readDataSkip(*serialization, istr, rows);

        ++pos;
    }

    //    if (rows && header)
    //    {
    //        /// Allow to skip columns. Fill them with default values.
    //        Block tmp_res;
    //
    //        for (auto & col : header)
    //        {
    //            if (res.has(col.name))
    //                tmp_res.insert(res.getByName(col.name));
    //            else
    //                tmp_res.insert({col.type->createColumn()->cloneResized(rows), col.type, col.name});
    //        }
    //
    //        res.swap(tmp_res);
    //    }

    return res;
}
}
