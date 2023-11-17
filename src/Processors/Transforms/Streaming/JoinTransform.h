#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <base/SerdeTag.h>

namespace DB
{
class NotJoinedBlocks;

namespace Streaming
{
/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently and have watermark / timestamp
/// alignment for temporal join scenarios.
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
    void handleInputsAlignment();
    int64_t getWatermark(const Chunk & chunk) const;

    void onCancel() override;

private:
    struct InputPortWithData
    {
        explicit InputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        std::list<Chunk> input_chunks;

        /// For join transform, we keep track watermark by itself
        int64_t watermark = std::numeric_limits<int64_t>::min();
        bool muted = false;
        bool required_checkpoint = false;
    };

    std::optional<size_t> required_update_processing_index;

    SERDE HashJoinPtr join;
    bool range_bidirectional_hash_join = false;
    bool bidirectional_hash_join = false;
    bool require_inputs_alignment = false;

    [[maybe_unused]] size_t max_block_size;

    Chunk output_header_chunk;
    size_t watermark_column_pos = 0;
    int64_t latency_threshold = 500;

    mutable std::mutex mutex;

    /// When received request checkpoint, it's always empty chunk with checkpoint context
    NO_SERDE std::array<InputPortWithData, 2> input_ports_with_data;
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE std::list<Chunk> output_chunks;

    SERDE int64_t watermark = INVALID_WATERMARK;

    struct AlignmentStats
    {
        uint64_t left_input_muted = 0;
        uint64_t right_right_muted = 0;
    };
    Poco::Logger * logger;
};
}
}
