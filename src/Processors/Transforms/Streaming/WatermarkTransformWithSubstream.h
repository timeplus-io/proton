#pragma once

#include <Processors/Transforms/Streaming/WatermarkStamper.h>

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
    WatermarkTransformWithSubstream(const Block & header, WatermarkStamperParams params, Poco::Logger * log);

    ~WatermarkTransformWithSubstream() override = default;

    String getName() const override { return watermark_template->getName() + "TransformWithSubstream"; }
    Status prepare() override;
    void work() override;
    void checkpoint(CheckpointContextPtr) override;
    void recover(CheckpointContextPtr) override;


private:
    inline WatermarkStamper & getOrCreateSubstreamWatermark(const SubstreamID & id);

    Chunk input_chunk;
    Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    WatermarkStamperPtr watermark_template;
    SubstreamHashMap<WatermarkStamperPtr> substream_watermarks;

    Poco::Logger * log;
};
}
}
