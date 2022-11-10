#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{
class WriteBuffer;

/** Serializes the stream of blocks in their native binary format (with names and column types).
  * Designed for checkpointing
  */
class SimpleNativeWriter
{
public:
    /// If non-zero client_revision is specified, additional block information can be written.
    SimpleNativeWriter(WriteBuffer & ostr_, UInt64 client_revision_);

    void write(const Block & block);
    void flush();

private:
    WriteBuffer & ostr;
    UInt64 client_revision;
};

}
