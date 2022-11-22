#pragma once

#include "Watermark.h"

#include <Processors/ISimpleTransform.h>

namespace DB
{
/**
 * WatermarkTransform projects watermark according to watermark strategies
 * by observing the events in its input.
 */

namespace Streaming
{
class WatermarkTransform final : public ISimpleTransform
{
public:
    WatermarkTransform(
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        FunctionDescriptionPtr desc,
        bool proc_time,
        const Block & input_header,
        const Block & output_header,
        Poco::Logger * log);

    ~WatermarkTransform() override = default;

    String getName() const override { return watermark->getName() + "Transform"; }

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

private:
    void initWatermark(
        const Block & input_header,
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        FunctionDescriptionPtr desc,
        bool proc_time,
        Poco::Logger * log);

    WatermarkPtr watermark;
};
}
}
