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
    {
        auto chunk_ctx = std::make_shared<ChunkContext>();
        chunk_ctx->setWatermark(Streaming::WatermarkBound{Streaming::INVALID_SUBSTREAM_ID, block.info.watermark, block.info.watermark_lower_bound});
        chunk.setChunkContext(std::move(chunk_ctx));
    }
    /// proton: ends

    return chunk;
}

}
