#pragma once

#include <Core/HashTableType.h>
#include <Interpreters/Streaming/Substream/ISubstreamHashMap.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/Streaming/WatermarkStamper.h>

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
        const Block & header,
        EmitParamsPtr params_,
        bool skip_stamping_for_backfill_data_,
        HashTableType hash_table_type,
        const String & spill_dir,
        size_t max_hot_key_count,
        const String & kv_options);

    ~WatermarkTransformWithSubstream() override = default;

    String getName() const override;
    Status prepare() override;
    void work() override;
    bool hasState() const override { return true; }
    void doCheckpoint(CheckpointContextPtr) override;
    void doRecover(CheckpointContextPtr) override;


private:
    inline WatermarkStamper & getOrCreateSubstreamWatermark(const SubstreamID & id);
    inline bool removeSubstreamWatermark(const SubstreamID & id);

    bool releaseConsecutivePartnerIfDifferent(const SubstreamID & was_consecutive_id, const SubstreamID & current_id);

    void initSubstreamHashMap(HashTableType hash_table_type, const String & spill_dir, size_t max_hot_key_count, const String & kv_options);

    Chunk input_chunk;
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    EmitParamsPtr params;
    WatermarkStamperPtr watermark_template;

    using ValueType = WatermarkStamperPtr;
    SERDE SubstreamHashMapPtr substream_watermarks;

    bool skip_stamping_for_backfill_data;
    bool mute_watermark = false;

    /// INVALID_SUBSTREAM_ID when the previous chunk was not a consecutive-begin.
    /// Not serialized: a consecutive pair cannot straddle a checkpoint barrier.
    SubstreamID prev_consecutive_id = INVALID_SUBSTREAM_ID;

    LoggerPtr logger;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;
};
}
}
