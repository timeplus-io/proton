#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
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
        size_t max_block_size_,
        UInt64 join_max_cached_bytes_);

    String getName() const override { return "StreamingJoinTransform"; }
    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    using Chunks = std::array<Chunk, 2>;
    void propagateWatermark(int64_t local_watermark);
    bool setupWatermark(Chunk & chunk, int64_t local_watermark);

    void doJoin(Chunks chunks);
    void joinBidirectionally(Chunks chunks);
    void rangeJoinBidirectionally(Chunks chunks);

    void onCancel() override;

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

    [[maybe_unused]] std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
    [[maybe_unused]] size_t max_block_size;

    Chunk output_header_chunk;

    Poco::Logger * logger;

    mutable std::mutex mutex;

    /// When received request checkpoint, it's always empty chunk with checkpoint context
    NO_SERDE std::array<InputPortWithData, 2> input_ports_with_data;
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE std::list<Chunk> output_chunks;

    SERDE int64_t watermark = INVALID_WATERMARK;

    NO_SERDE Int64 last_log_ts = 0;
};
}
}
