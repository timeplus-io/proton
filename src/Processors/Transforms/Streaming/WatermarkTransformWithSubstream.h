#pragma once

#include "Watermark.h"

#include <Processors/IProcessor.h>
#include <Processors/Streaming/ChunkSplitter.h>

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
        std::vector<size_t> key_column_positions,
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

    inline std::pair<Int64, Int64> calcMinMaxEventTime(const Chunk & chunk) const;
    inline Watermark & getOrCreateSubstreamWatermark(const SubstreamID & id);

    Block header;
    Chunk input_chunk;
    Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    ChunkSplitter substream_splitter;

    String watermark_name;
    WatermarkPtr watermark_template;
    SubstreamHashMap<WatermarkPtr> substream_watermarks;

    Poco::Logger * log;
};
}
}
