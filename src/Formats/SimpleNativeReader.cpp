#include "SimpleNativeReader.h"

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}


SimpleNativeReader::SimpleNativeReader(ReadBuffer & istr_, UInt64 server_revision_)
    : istr(istr_), server_revision(server_revision_)
{
}

void SimpleNativeReader::readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
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
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeBlockInputStream. Rows read: {}. Rows expected: {}", column->size(), rows);
}

Block SimpleNativeReader::read()
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

        if (rows)    /// If no rows, nothing to read.
            readData(*serialization, read_column, istr, rows, 0);

        column.column = std::move(read_column);

        res.insert(std::move(column));
    }

    return res;
}
}
