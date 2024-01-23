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
    WatermarkTransformWithSubstream(
        const Block & header, WatermarkStamperParamsPtr params_, bool skip_stamping_for_backfill_data_, Poco::Logger * log);

    ~WatermarkTransformWithSubstream() override = default;

    String getName() const override;
    Status prepare() override;
    void work() override;
    void checkpoint(CheckpointContextPtr) override;
    void recover(CheckpointContextPtr) override;


private:
    inline WatermarkStamper & getOrCreateSubstreamWatermark(const SubstreamID & id);

    Chunk input_chunk;
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    WatermarkStamperParamsPtr params;
    WatermarkStamperPtr watermark_template;
    SERDE SubstreamHashMap<WatermarkStamperPtr> substream_watermarks;

    bool skip_stamping_for_backfill_data;
    bool mute_watermark = false;

    Poco::Logger * log;
};
}
}
