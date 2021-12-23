#pragma once

#include "Watermark.h"

#include <Processors/ISimpleTransform.h>

class DateLUTImpl;

namespace DB
{
/**
 * WatermarkTransform projects watermark according to watermark strategies
 * by observing the events in its input.
 */

class WatermarkTransform final : public ISimpleTransform
{
public:
    WatermarkTransform(
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        StreamingFunctionDescriptionPtr desc,
        bool proc_time,
        const Block & header,
        Poco::Logger * log);

    ~WatermarkTransform() override = default;

    String getName() const override { return "WatermarkTransform"; }

private:
    void transform(Chunk & chunk) override;

private:
    void initWatermark(
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        StreamingFunctionDescriptionPtr desc,
        bool proc_time,
        Poco::Logger * log);

    WatermarkPtr watermark;
};
}
