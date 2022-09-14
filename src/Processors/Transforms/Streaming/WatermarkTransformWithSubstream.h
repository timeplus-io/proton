#pragma once

#include "BlockSplitter.h"
#include "Watermark.h"

#include <Processors/IProcessor.h>

class DateLUTImpl;

namespace DB
{
/**
 * WatermarkTransform projects watermark according to watermark strategies
 * by observing the events in its input.
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
        const std::vector<size_t> & substream_keys,
        const Block & input_header,
        const Block & output_header,
        Poco::Logger * log);

    ~WatermarkTransformWithSubstream() override = default;

    String getName() const override { return watermark_name + "TransformWithSubstream"; }
    Status prepare() override;
    void work() override;

private:
    void initWatermark(
        ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time);

    Watermark & getOrCreateSubstreamWatermark(const SubstreamID & id);

    Poco::Logger * log;

    Block header;
    Chunk input_chunk;
    Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    Substream::BlockSplitter substream_splitter;
    String watermark_name;
    WatermarkPtr watermark_template;
    SubstreamHashMap<WatermarkPtr> substream_watermarks;
};
}
}
