#include "Record.h"

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
/// #include <DataStreams/materializeBlock.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DWAL
{
ByteVector Record::write(const Record & record)
{
    ByteVector data{static_cast<size_t>((record.block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};
    DB::NativeBlockOutputStream output(wb, 0, DB::Block{});

    /// Write flags
    /// flags bits distribution
    /// [0-4] : Version
    /// [5-10] : OpCode
    /// [11-63] : Reserved
    uint64_t flags = VERSION | (static_cast<UInt8>(record.op_code) << 5ul);
    DB::writeIntBinary(flags, wb);

    /// Data
    /// materializeBlockInplace(record.block);
    output.write(record.block);
    output.flush();

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

    DB::NativeBlockInputStream input{rb, 0};

    return std::make_shared<Record>(Record::opcode(flags), input.read());
}
}
