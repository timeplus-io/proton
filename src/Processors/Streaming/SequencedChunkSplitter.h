#pragma once

#include <Processors/Chunk.h>

namespace DB::Streaming
{
struct SequenceChunk
{
    Int64 sn;
    Chunk chunk;
};

/// SequencedChunkSplitter is used to split or concat chunk by sequence number.
class SequencedChunkSplitter
{
public:
    SequencedChunkSplitter(size_t sn_col_pos_, bool remove_sn_col_) : sn_col_pos(sn_col_pos_), remove_sn_col(remove_sn_col_) { }

    Int64 currentSN() const { return buffered_sn; }

    std::list<SequenceChunk> splitOrConcat(Chunk && chunk);

    /// Finalize the buffered chunk and return it as a SequenceChunk.
    SequenceChunk finalize();

private:
    size_t sn_col_pos;
    bool remove_sn_col;

    Int64 buffered_sn = -1;
    Chunk buffered_chunk;
};
}
