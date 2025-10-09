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
    UInt64 chunk_num = 0; // chunk number in order of generation, used during memory bound merging to restore chunks order
};

Chunk convertToChunk(const Block & block);
}
