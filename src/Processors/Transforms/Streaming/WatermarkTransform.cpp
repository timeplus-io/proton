#include "WatermarkTransform.h"
#include "HopWatermark.h"
#include "SessionWatermark.h"
#include "TumbleWatermark.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_EMIT_MODE;
}

namespace Streaming
{
WatermarkTransform::WatermarkTransform(
    ASTPtr query,
    TreeRewriterResultPtr syntax_analyzer_result,
    FunctionDescriptionPtr desc,
    bool proc_time,
    const Block & header,
    const Block & output_header,
    Poco::Logger * log)
    : ISimpleTransform(header, output_header, false, ProcessorID::WatermarkTransformID)
{
    initWatermark(query, syntax_analyzer_result, desc, proc_time, log);
    assert(watermark);
    watermark->preProcess();
}

void WatermarkTransform::transform(Chunk & chunk)
{
    if (auto ckpt_ctx = chunk.getCheckpointContext(); ckpt_ctx)
    {
        checkpoint(std::move(ckpt_ctx));
        /// Checkpoint barrier is always standalone
        /// Directly return automatically propagate the checkpoint barrier to down stream
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    watermark->process(block);
    chunk.setColumns(block.getColumns(), block.rows());

    if (block.hasWatermark())
    {
        auto chunk_ctx = chunk.getChunkContext();
        if (!chunk_ctx)
        {
            chunk_ctx = std::make_shared<ChunkContext>();
            chunk.setChunkContext(chunk_ctx);
        }
        chunk_ctx->setWatermark(WatermarkBound{INVALID_SUBSTREAM_ID, block.info.watermark, block.info.watermark_lower_bound});
    }
}

void WatermarkTransform::initWatermark(
    ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time, Poco::Logger * log)
{
    WatermarkSettings watermark_settings(query, syntax_analyzer_result, desc);
    if (watermark_settings.func_name == ProtonConsts::TUMBLE_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark = std::make_unique<TumbleWatermark>(std::move(watermark_settings), proc_time, log);
    }
    else if (watermark_settings.func_name == ProtonConsts::HOP_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark = std::make_unique<HopWatermark>(std::move(watermark_settings), proc_time, log);
    }
    else if (watermark_settings.func_name == ProtonConsts::SESSION_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark
            = std::make_unique<SessionWatermark>(std::move(watermark_settings), proc_time, desc->session_start, desc->session_end, log);
    }
    else
    {
        watermark = std::make_unique<Watermark>(std::move(watermark_settings), proc_time, log);
    }
}

void WatermarkTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), logic_pid, ckpt_ctx, [this](WriteBuffer & wb) { watermark->serialize(wb); });
}

void WatermarkTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(logic_pid, ckpt_ctx, [this](VersionType, ReadBuffer & rb) { watermark->deserialize(rb); });
}

}
}
