#pragma once

#include "Watermark.h"

#include <Processors/IProcessor.h>

namespace DB
{
/**
 * WatermarkTransformWithSubstream partitions data according to substream key columns and then
 * projects watermark according to watermark strategies by observing the events in each substream.
 */

namespace Streaming
{
class WatermarkTransformWithSubstream final : public IProcessor
{
public:
    WatermarkTransformWithSubstream(
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        FunctionDescriptionPtr desc,
        bool proc_time,
        const Block & input_header,
        const Block & output_header,
        Poco::Logger * log);

    ~WatermarkTransformWithSubstream() override = default;

    String getName() const override { return watermark_name + "TransformWithSubstream"; }
    Status prepare() override;
    void work() override;
    void checkpoint(CheckpointContextPtr) override;
    void recover(CheckpointContextPtr) override;


private:
    void initWatermark(
        const Block & input_header,
        ASTPtr query,
        TreeRewriterResultPtr syntax_analyzer_result,
        FunctionDescriptionPtr desc,
        bool proc_time);

    inline Watermark & getOrCreateSubstreamWatermark(const SubstreamID & id);

    Block header;
    Chunk input_chunk;
    Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    String watermark_name;
    WatermarkPtr watermark_template;
    SubstreamHashMap<WatermarkPtr> substream_watermarks;

    Poco::Logger * log;
};
}
}
