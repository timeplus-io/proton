#pragma once

#include <Processors/Chunk.h>

namespace DB
{
class Block;

class AggregatedChunkInfo : public ChunkInfo
{
public:
    bool is_overflows = false;
    Int32 bucket_num = -1;
};

Chunk convertToChunk(const Block & block);
}
