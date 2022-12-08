#include "SimpleNativeWriter.h"

#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


SimpleNativeWriter::SimpleNativeWriter(WriteBuffer & ostr_, UInt64 client_revision_) : ostr(ostr_), client_revision(client_revision_)
{
}

void SimpleNativeWriter::flush()
{
    ostr.next();
}

static void writeData(const ISerialization & serialization, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
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

void SimpleNativeWriter::write(const Block & block)
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

        const auto * aggregate_function_data_type = typeid_cast<const DataTypeAggregateFunction *>(column.type.get());
        if (aggregate_function_data_type && aggregate_function_data_type->isVersioned())
        {
            auto version = aggregate_function_data_type->getVersionFromRevision(client_revision);
            aggregate_function_data_type->setVersion(version, /* if_empty */ true);
        }

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
}
