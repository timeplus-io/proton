#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{
class Chunk;
class WriteBuffer;

/** Serializes the stream of blocks in their native binary format (with names and column types).
  * Designed for checkpointing
  */
template <typename DataBlock>
requires(std::is_same_v<DataBlock, Block> || std::is_same_v<DataBlock, Chunk>)
class SimpleNativeWriter
{
public:
    /// If non-zero client_revision is specified, additional block information can be written.
    SimpleNativeWriter(WriteBuffer & ostr_, const Block & header_, UInt64 client_revision_)
        : ostr(ostr_), header(header_.cloneEmpty()), client_revision(client_revision_)
    {
    }

    void write(const DataBlock & data_block);
    void flush();

private:
    WriteBuffer & ostr;
    Block header;
    UInt64 client_revision;
};

void writeBlock(const Block & block, UInt64 client_revision, WriteBuffer & ostr);
void writeChunk(const Chunk & chunk, const Block & header, UInt64 client_revision, WriteBuffer & ostr);
}
