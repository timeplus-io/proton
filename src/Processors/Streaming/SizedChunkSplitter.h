#pragma once

#include <Columns/IColumn.h>
#include <Core/BlockWithShard.h>


namespace DB
{
namespace Streaming
{
/// ChunkSplitter is used to split block by max block size (rows) and bytes.
class SizedChunkSplitter
{
public:
    SizedChunkSplitter(UInt64 max_rows_, UInt64 max_bytes_) : max_rows(max_rows_), max_bytes(max_bytes_) { }

    /// Split block by max_size / max_bytes.
    /// Source block rows may belong to different shards. Each splitted block contains only the rows of the same shard.
    /// `selector` is passed with the row shard ID. It is reused and modified in this function to avoid extra allocation.
    BlocksWithShard splitBlock(Block block, UInt32 num_shards, IColumn::Selector & selector) const;

    /// Optimized block splitting process for all rows belong to a single shard.
    BlocksWithShard splitBlockForSingleShard(Block block, UInt32 shard) const;

private:
    UInt64 max_rows;
    UInt64 max_bytes;
};
}
}
