#pragma once

#include "Watermark.h"

#include <Processors/Streaming/ChunkSplitter.h>
#include <Processors/IProcessor.h>

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
        std::vector<size_t> key_column_posistions,
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
        ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time);

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

    /// For SessionWatermark
    bool emit_min_max_event_time = false;
    bool time_col_is_datetime64 = true;
    size_t time_col_pos = 0;

    Poco::Logger * log;
};
}
}
