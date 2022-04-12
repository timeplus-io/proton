#include "SchemaNativeWriter.h"

#include <Core/Block.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/WriteHelpers.h>

namespace nlog
{
namespace
{
    ALWAYS_INLINE void writeData(
        const DB::ISerialization & serialization, const DB::ColumnPtr & column, DB::WriteBuffer & ostr, uint64_t offset, uint64_t limit)
    {
        /// If there are columns-constants - then we materialize them.
        /// (Since the data type does not know how to serialize / deserialize constants.)
        DB::ColumnPtr full_column = column->convertToFullColumnIfConst();
        DB::ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&ostr](DB::ISerialization::SubstreamPath) -> DB::WriteBuffer * { return &ostr; };
        settings.position_independent_encoding = false;
        settings.low_cardinality_max_dictionary_size = 0; //-V1048

        DB::ISerialization::SerializeBinaryBulkStatePtr state;
        serialization.serializeBinaryBulkStatePrefix(settings, state);
        serialization.serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
        serialization.serializeBinaryBulkStateSuffix(settings, state);
    }
}

SchemaNativeWriter::SchemaNativeWriter(DB::WriteBuffer & ostr_, const std::vector<uint16_t> & column_positions_)
    : ostr(ostr_), column_positions(column_positions_)
{
}

/// We assume columns in block is sorted according to schema metadata
void SchemaNativeWriter::write(const DB::Block & block)
{
    /// block.checkNumberOfRows();

    /// Dimensions
    /// We don't support these many columns and rows in one block
    uint16_t columns = block.columns();
    uint32_t rows = block.rows();

    assert(columns > 0);
    assert(column_positions.empty() || column_positions.size() == columns);
    assert(rows > 0);

    DB::writeIntBinary(columns, ostr);
    DB::writeIntBinary(rows, ostr);

    /// Column index in the schema
    for (auto pos : column_positions)
        DB::writeIntBinary(pos, ostr);

    for (const auto & column : block)
    {
        /// Serialization. Dynamic, if client supports it.
        auto info = column.column->getSerializationInfo();
        auto serialization = column.type->getSerialization(*info);
        bool has_custom = info->hasCustomSerialization();

        writeIntBinary(static_cast<uint8_t>(has_custom), ostr);
        if (has_custom)
            info->serialializeKindBinary(ostr);

        /// Data
        writeData(*serialization, column.column, ostr, 0, 0);
    }
}
}
