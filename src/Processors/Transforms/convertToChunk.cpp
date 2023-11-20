#include "convertToChunk.h"

#include <Core/Block.h>

namespace DB
{

/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    /// proton: starts
    if (block.hasWatermark())
        chunk.setWatermark(block.watermark());
    /// proton: ends

    return chunk;
}

}
