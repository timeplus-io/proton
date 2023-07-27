#pragma once

#include <Core/Block.h>
#include <Formats/IndexForNativeFormat.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/PODArray.h>

namespace DB
{
class Chunk;
class ReadBuffer;

/** Deserializes the stream of blocks from the native binary format (with names and column types).
  * Designed for checkpointing
  */
template <typename DataBlock>
    requires(std::is_same_v<DataBlock, Block> || std::is_same_v<DataBlock, Chunk>)
class SimpleNativeReader
{
public:
    /// If a non-zero server_revision is specified, additional block information may be expected and read.
    SimpleNativeReader(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_)
        : istr(istr_), header(header_), server_revision(server_revision_)
    {
    }

    DataBlock read();

private:
    ReadBuffer & istr;
    Block header;
    UInt64 server_revision;
};

Block readBlock(UInt64 server_revision, ReadBuffer & istr);
Chunk readChunk(const Block & header, UInt64 server_revision, ReadBuffer & istr);
}
