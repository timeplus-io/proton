#include "WatermarkTransform.h"
#include "HopWatermark.h"
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
    const Block & input_header,
    const Block & output_header,
    Poco::Logger * log)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::WatermarkTransformID)
{
    initWatermark(input_header, query, syntax_analyzer_result, desc, proc_time, log);
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

    if (!chunk.avoidWatermark())
        watermark->process(chunk);
}

void WatermarkTransform::initWatermark(
    const Block & input_header, ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time, Poco::Logger * log)
{
    WatermarkSettings watermark_settings(query, syntax_analyzer_result, desc);
    if (watermark_settings.isTumbleWindowAggr())
    {
        assert(watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK || watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY);
        auto time_col_position = input_header.getPositionByName(desc->argument_names[0]);
        watermark = std::make_unique<TumbleWatermark>(std::move(watermark_settings), time_col_position, proc_time, log);
    }
    else if (watermark_settings.isHopWindowAggr())
    {
        assert(watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK || watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY);

        auto time_col_position = input_header.getPositionByName(desc->argument_names[0]);
        watermark = std::make_unique<HopWatermark>(std::move(watermark_settings), time_col_position, proc_time, log);
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
