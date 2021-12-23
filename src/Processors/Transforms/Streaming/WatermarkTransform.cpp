#include "WatermarkTransform.h"
#include "HopWatermark.h"
#include "TumbleWatermark.h"

/// FIXME: Week / Month / Quarter / Year cases don't work yet
namespace DB
{
WatermarkTransform::WatermarkTransform(
    ASTPtr query,
    TreeRewriterResultPtr syntax_analyzer_result,
    StreamingFunctionDescriptionPtr desc,
    bool proc_time,
    const Block & header,
    Poco::Logger * log)
    : ISimpleTransform(header, header, false)
{
    initWatermark(query, syntax_analyzer_result, desc, proc_time, log);
    assert(watermark);
    watermark->preProcess();
}

void WatermarkTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    watermark->process(block);
    chunk.setColumns(block.getColumns(), block.rows());
    if (block.info.watermark != 0)
    {
        auto chunk_info = chunk.getChunkInfo();
        if (!chunk_info)
        {
            chunk.setChunkInfo(std::make_shared<ChunkInfo>());
            chunk_info = chunk.getChunkInfo();
        }
        const_cast<ChunkInfo *>(chunk_info.get())->ctx.setWatermark(block.info.watermark, block.info.watermark_lower_bound);
    }
}

void WatermarkTransform::initWatermark(
    ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, StreamingFunctionDescriptionPtr desc, bool proc_time, Poco::Logger * log)
{
    WatermarkSettings watermark_settings(query, syntax_analyzer_result, desc);
    if (watermark_settings.func_name == "__TUMBLE")
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::SYNTAX_ERROR);

        watermark = std::make_shared<TumbleWatermark>(std::move(watermark_settings), proc_time, log);
    }
    else if (watermark_settings.func_name == "__HOP")
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::SYNTAX_ERROR);

        watermark = std::make_shared<HopWatermark>(std::move(watermark_settings), proc_time, log);
    }
    else
    {
        watermark = std::make_shared<Watermark>(std::move(watermark_settings), proc_time, log);
    }
}
}
