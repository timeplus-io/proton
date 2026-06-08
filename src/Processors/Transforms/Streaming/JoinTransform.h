#pragma once

#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <Common/serde.h>

namespace DB
{
class NotJoinedBlocks;

namespace Streaming
{
class JoinRightBoundary;
using JoinRightBoundaryPtr = std::shared_ptr<JoinRightBoundary>;

class JoinLeftBoundary;
using JoinLeftBoundaryPtr = std::shared_ptr<JoinLeftBoundary>;

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
        UInt64 join_max_cached_bytes_,
        JoinRightBoundaryPtr right_boundary_ = nullptr,
        JoinLeftBoundaryPtr left_boundary_ = nullptr);

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
    void processLeftChunk(Chunk chunk);
    bool markRightBoundaryReady(const char * reason);
    bool observeLeftBoundaryReleased(const char * reason);
    void replayDelayedLeftChunks();
    void processDelayedRightChunks();
    bool hasCheckpointUnsafeBoundaryState() const;
    void abandonBoundaryParticipation(const char * reason) noexcept;
    void markBoundaryParticipationAbandoned(const char * reason) noexcept;

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
    NO_SERDE JoinRightBoundaryPtr right_boundary;
    NO_SERDE JoinLeftBoundaryPtr left_boundary;
    NO_SERDE bool right_boundary_ready = false;
    NO_SERDE bool right_boundary_local_reached = false;
    NO_SERDE bool left_boundary_released = false;
    NO_SERDE ChunkList delayed_left_chunks;
    NO_SERDE ChunkList delayed_right_chunks;

    NO_SERDE Int64 last_log_ts = 0;
};
}
}
