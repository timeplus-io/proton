#pragma once

#include <Formats/IndexForNativeFormat.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{
/** Deserializes the stream of blocks from the native binary format (with names and column types).
  * Designed for checkpointing
  */
class SimpleNativeReader
{
public:
    /// If a non-zero server_revision is specified, additional block information may be expected and read.
    SimpleNativeReader(ReadBuffer & istr_, UInt64 server_revision_);

    static void readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint);

    Block read();

private:
    ReadBuffer & istr;
    UInt64 server_revision;
};

}
