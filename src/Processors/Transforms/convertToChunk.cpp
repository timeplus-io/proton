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

    /// proton: starts
    info->ctx.setWatermark(block.info.watermark, block.info.watermark_lower_bound);
    /// proton: ends

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    return chunk;
}

}
