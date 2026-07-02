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
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    using Chunks = std::array<Chunk, 2>;
    bool setupWatermark(Chunk & chunk, int64_t local_watermark);

    void doJoin(Chunks chunks);
    void joinBidirectionally(Chunks chunks);
    void rangeJoinBidirectionally(Chunks chunks);
    void advanceBackfillState(const Chunk & right_chunk);

    void onCancel() noexcept override;

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

    static constexpr Int64 log_metrics_interval_ms = 60'000;
    NO_SERDE Int64 last_log_ts = 0;

    /// Right-side backfill state machine for enrichment joins.
    /// For non-bidirectional joins, we must ensure the right-side hash table is fully
    /// populated before processing any left-side data.
    ///
    /// States:
    ///   Pending     — initial; hold left input, only pull right to determine if backfill exists
    ///   Backfilling — START marker received; keep holding left until END marker
    ///   Done        — backfill complete or confirmed absent; both sides run freely
    ///
    /// Transitions:
    ///   Pending → Backfilling  : right input sends HISTORICAL_DATA_START marker
    ///   Pending → Done         : right input sends data/heartbeat without START marker,
    ///                            or right input has no data while left has data (no backfill)
    ///   Backfilling → Done     : right input sends HISTORICAL_DATA_END marker
    enum class BackfillState : uint8_t
    {
        Pending,
        Backfilling,
        Done,
    };
    NO_SERDE BackfillState backfill_right_state = BackfillState::Pending;

    bool canConsumeLeftStream() const { return bidirectional_hash_join || backfill_right_state == BackfillState::Done; }
};
}
}
