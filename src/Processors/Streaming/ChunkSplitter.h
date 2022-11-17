#pragma once

#include <Core/Types.h>
#include <Processors/Chunk.h>

#include <vector>

namespace DB
{
namespace Streaming
{

struct ChunkWithID
{
    ChunkWithID(UInt128 id_, Chunk chunk_) : id(std::move(id_)), chunk(std::move(chunk_)) { }

    UInt128 id;
    Chunk chunk;
};

/// Split block via keys columns
class ChunkSplitter final
{
public:
    explicit ChunkSplitter(std::vector<size_t> key_column_positions_);

    std::vector<ChunkWithID> split(Chunk & chunk) const;

private:
    std::vector<ChunkWithID> splitOneRow(Chunk & chunk) const;

private:
    std::vector<size_t> key_column_positions;
};

}
}
