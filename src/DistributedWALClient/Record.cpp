#include "Record.h"
#include "SchemaNativeReader.h"
#include "SchemaNativeWriter.h"

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DWAL
{
ByteVector Record::write(const Record & record, DB::CompressionMethodByte codec)
{
    if (likely(record.hasSchema()))
        return writeInSchema(record, codec);

    ByteVector data{static_cast<size_t>((record.block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};

    /// Write flags
    uint64_t wire_flags = flags(Version::NATIVE, record.op_code, codec);
    DB::writeIntBinary(wire_flags, wb);

    /// Data
    /// materializeBlockInplace(record.block);
    if (likely(codec == DB::CompressionMethodByte::NONE))
    {
        DB::NativeWriter writer(wb, DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION, DB::Block{});
        writer.write(record.block);
        writer.flush();
    }
    else
    {
        DB::CompressedWriteBuffer compressed_out = DB::CompressedWriteBuffer(
            wb, DB::CompressionCodecFactory::instance().get(static_cast<uint8_t>(codec)), DBMS_DEFAULT_BUFFER_SIZE);
        DB::NativeWriter writer(compressed_out, DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION, DB::Block{});
        writer.write(record.block);
        writer.flush();
    }

    /// Shrink to what has been written
    wb.finalize();
    return data;
}

RecordPtr Record::read(const char * data, size_t size, const SchemaContext & schema_ctx)
{
    DB::ReadBufferFromMemory rb{data, size};

    uint64_t wire_flags = 0;
    readIntBinary(wire_flags, rb);

    auto version = Record::version(wire_flags);
    if (version == Version::NATIVE_IN_SCHEMA)
        return readInSchema(rb, wire_flags, false, schema_ctx);
    else if (version == Version::NATIVE_IN_SCHEMA_PARTIAL)
        return readInSchema(rb, wire_flags, true, schema_ctx);

    auto method = codec(wire_flags);
    if (likely(method == DB::CompressionMethodByte::NONE))
    {
        DB::NativeReader reader(rb, DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION);
        return std::make_shared<Record>(Record::opcode(wire_flags), reader.read(), NO_SCHEMA);
    }
    else
    {
        DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
        DB::NativeReader reader(compressed_in, DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION);
        return std::make_shared<Record>(Record::opcode(wire_flags), reader.read(), NO_SCHEMA);
    }
}

ByteVector Record::writeInSchema(const Record & record, DB::CompressionMethodByte codec)
{
    assert(record.hasSchema());

    ByteVector data{static_cast<size_t>((record.block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};

    auto version = record.column_positions.empty() ? Version::NATIVE_IN_SCHEMA : Version::NATIVE_IN_SCHEMA_PARTIAL;
    uint64_t wire_flags = flags(version, record.op_code, codec);
    DB::writeIntBinary(wire_flags, wb);

    /// Data
    /// materializeBlockInplace(record.block);
    if (likely(codec == DB::CompressionMethodByte::NONE))
    {
        SchemaNativeWriter writer(wb, record.schema(), record.column_positions);
        writer.write(record.block);
        writer.flush();
    }
    else
    {
        DB::CompressedWriteBuffer compressed_out = DB::CompressedWriteBuffer(
            wb, DB::CompressionCodecFactory::instance().get(static_cast<uint8_t>(codec)), DBMS_DEFAULT_BUFFER_SIZE);
        SchemaNativeWriter writer(compressed_out, record.schema(), record.column_positions);
        writer.write(record.block);
        writer.flush();
    }

    /// Shrink to what has been written
    wb.finalize();
    return data;
}

RecordPtr Record::readInSchema(DB::ReadBufferFromMemory & rb, uint64_t flags, bool partial, const SchemaContext & schema_ctx)
{
    assert(
        (partial && (version(flags) == Version::NATIVE_IN_SCHEMA_PARTIAL)) || (!partial && (version(flags) == Version::NATIVE_IN_SCHEMA)));

    uint16_t schema_ver = 0;
    if (likely(codec(flags) == DB::CompressionMethodByte::NONE))
    {
        SchemaNativeReader reader(rb, schema_ver, partial, schema_ctx);
        return std::make_shared<Record>(Record::opcode(flags), reader.read(), schema_ver);
    }
    else
    {
        DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
        SchemaNativeReader reader(compressed_in, schema_ver, partial, schema_ctx);
        return std::make_shared<Record>(Record::opcode(flags), reader.read(), schema_ver);
    }
}
}
