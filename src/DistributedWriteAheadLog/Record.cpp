#include "Record.h"

#include <Formats//NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DWAL
{
ByteVector Record::write(const Record & record, bool compressed)
{
    ByteVector data{static_cast<size_t>((record.block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};

    /// Write flags
    /// flags bits distribution
    /// [0-4] : Version
    /// [5-10] : OpCode
    /// [11] : Compression
    /// [12-63] : Reserved
    uint64_t flags = VERSION | (static_cast<UInt8>(record.op_code) << 5ul) | (static_cast<UInt8>(compressed)) << 11ul;
    DB::writeIntBinary(flags, wb);

    /// Data
    /// materializeBlockInplace(record.block);
    if (unlikely(compressed))
    {
        DB::CompressedWriteBuffer compressed_out
            = DB::CompressedWriteBuffer(wb, DB::CompressionCodecFactory::instance().get("LZ4", {}), DBMS_DEFAULT_BUFFER_SIZE);
        DB::NativeWriter writer(compressed_out, 0, DB::Block{});
        writer.write(record.block);
        writer.flush();
    }
    else
    {
        DB::NativeWriter writer(wb, 0, DB::Block{});
        writer.write(record.block);
        writer.flush();
    }

    /// Shrink to what has been written
    wb.finalize();
    return data;
}

RecordPtr Record::read(const char * data, size_t size)
{
    DB::ReadBufferFromMemory rb{data, size};

    UInt64 flags = 0;
    readIntBinary(flags, rb);

    /// FIXME, more graceful version handling
    assert(Record::version(flags) == VERSION);

    if (unlikely(Record::compression(flags)))
    {
        DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
        DB::NativeReader reader(compressed_in, 0);
        return std::make_shared<Record>(Record::opcode(flags), reader.read());
    }
    else
    {
        DB::NativeReader reader(rb, 0);
        return std::make_shared<Record>(Record::opcode(flags), reader.read());
    }
}
}

