#include "IDistributedWriteAheadLog.h"

#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void IDistributedWriteAheadLog::startup()
{
}

void IDistributedWriteAheadLog::shutdown()
{
}

ByteVector IDistributedWriteAheadLog::Record::write(const Record & record)
{
    ByteVector data{static_cast<size_t>((record.block.bytes() + 2) * 1.5)};

    WriteBufferFromVector wb{data};
    auto output = std::make_unique<MaterializingBlockOutputStream>(std::make_shared<NativeBlockOutputStream>(wb, 0, Block{}), Block{});

    /// Write flags
    /// flags bits distribution
    /// [0-4] : Version
    /// [5-10] : OpCode
    /// [11-63] : Reserved
    UInt64 flags = (IDistributedWriteAheadLog::WAL_VERSION) | (static_cast<UInt8>(record.op_code) << 5ul);
    writeIntBinary(flags, wb);

    /// Data
    output->write(record.block);
    output->flush();

    /// shrink to what has been written
    wb.finalize();
    return data;
}

IDistributedWriteAheadLog::RecordPtr IDistributedWriteAheadLog::Record::read(const char * data, size_t size)
{
    ReadBufferFromMemory rb{data, size};

    UInt64 flags = 0;
    readIntBinary(flags, rb);

    /// FIXME, more graceful version handling
    assert(Record::version(flags) == IDistributedWriteAheadLog::WAL_VERSION);

    NativeBlockInputStream input{rb, 0};

    return std::make_shared<IDistributedWriteAheadLog::Record>(Record::opcode(flags), input.read());
}
}
