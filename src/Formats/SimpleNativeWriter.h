#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{
class Chunk;
struct LightChunk;
struct LightChunkWithTimestamp;
class WriteBuffer;

/** Serializes the stream of blocks in their native binary format (with names and column types).
  * Designed for checkpointing
  */
template <typename DataBlock>
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
void writeLightChunk(const LightChunk & data, const Block & header, UInt64 client_revision, WriteBuffer & ostr);
void writeLightChunkWithTimestamp(const LightChunkWithTimestamp & data, const Block & header, UInt64 client_revision, WriteBuffer & ostr);

using NativeBlockWriter = SimpleNativeWriter<Block>;
using NativeChunkWriter = SimpleNativeWriter<Chunk>;
using NativeLightChunkWriter = SimpleNativeWriter<LightChunk>;
using NativeLightChunkWithTimestampWriter = SimpleNativeWriter<LightChunkWithTimestamp>;
}
