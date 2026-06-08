#pragma once

#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <Common/serde.h>

namespace DB
{
class NotJoinedBlocks;

namespace Streaming
{
/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently
/// left stream -> ... ->
///                      \
///                      JoinTransform
///                      /
/// right stream -> ... ->
class JoinTransform final : public IProcessor
{
public:
    JoinTransform(
        Block left_input_header,
        Block right_input_header,
        Block output_header,
        HashJoinPtr join_,
        size_t transform_id_,
        size_t max_block_size_,
        UInt64 join_max_cached_bytes_);

    String getName() const override;
    Status prepare() override;
    void work() override;

    bool hasState() const override { return true; }
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    using Chunks = std::array<Chunk, 2>;
    bool setupWatermark(Chunk & chunk, int64_t local_watermark);

    void doJoin(Chunks chunks);
    void joinBidirectionally(Chunks chunks);
    void rangeJoinBidirectionally(Chunks chunks);

    void onCancel() noexcept override;

private:
    struct InputPortWithData
    {
        explicit InputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        Chunk input_chunk;
    };

    std::optional<size_t> required_update_processing_index;

    /// std::atomic_bool stop_reading = false;
    [[maybe_unused]] bool process_non_joined = true;

    SERDE HashJoinPtr join;
    bool range_bidirectional_hash_join = false;
    bool bidirectional_hash_join = false;

    /// Historical-backfill ordering for the non-bidirectional (data-enrichment, e.g. INNER/LEFT
    /// LATEST / ASOF) join path. In that path the left side probes the right (build-side) hash table
    /// and is NOT buffered, so a left historical row that probes before the matching right historical
    /// row has been inserted is dropped permanently. When both sides backfill from history (e.g.
    /// `seek_to='earliest'`) whichever side the executor happens to schedule first decides the result,
    /// which is non-deterministic. To make it deterministic we hold the left side's historical rows
    /// until the right side's historical backfill has completed. Pure live joins emit no historical
    /// markers, so `left_in_historical_backfill` is never set there and the gate stays disabled.
    /// These are one-shot startup flags (the right side does not re-backfill after recovery), so they
    /// are intentionally not part of the checkpoint state.
    NO_SERDE bool gate_left_on_right_backfill = false;
    NO_SERDE bool left_in_historical_backfill = false;
    NO_SERDE bool right_backfill_started = false;
    NO_SERDE bool right_historical_backfill_done = false;

    /// True while a left historical-backfill row must wait for the right side's historical backfill.
    bool leftHistoricalDataGated() const
    {
        return gate_left_on_right_backfill && left_in_historical_backfill && !right_historical_backfill_done;
    }

    /// Update the historical-backfill flags from a chunk observed on input `input_index` (0=left, 1=right).
    void trackHistoricalBackfill(size_t input_index, const Chunk & chunk);

    size_t transform_id;
    [[maybe_unused]] std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
    [[maybe_unused]] size_t max_block_size;

    Chunk output_header_chunk;

    LoggerPtr logger;

    /// When received request checkpoint, it's always empty chunk with checkpoint context
    NO_SERDE std::array<InputPortWithData, 2> input_ports_with_data;
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE ChunkList output_chunks;

    SERDE int64_t watermark = INVALID_WATERMARK;

    NO_SERDE Int64 last_log_ts = 0;
};
}
}
