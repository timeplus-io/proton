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
///                      JoinTransformWithAlignment
///                      /
/// right stream -> ... ->
class JoinTransformWithAlignment final : public IProcessor
{
public:
    JoinTransformWithAlignment(
        Block left_input_header,
        Block right_input_header,
        Block output_header,
        HashJoinPtr join_,
        UInt64 join_max_cached_bytes_);

    String getName() const override { return "StreamingJoinTransformWithAlignment"; }
    Status prepare() override;
    void work() override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    Status prepareLeftInput();
    Status prepareRightInput();
    Int64 getWatermark(const Chunk & chunk) const;
    void onCancel() override;

private:
    struct LeftInputPortWithData
    {
        explicit LeftInputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        std::list<Chunk> input_chunks;

        /// For join transform, we keep track watermark by itself
        Int64 watermark = INVALID_WATERMARK;
        bool muted = false;
        bool required_checkpoint = false;
    };

    struct RightInputPortWithData
    {
        explicit RightInputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        Chunk input_chunk;

        /// For join transform, we keep track watermark by itself
        Int64 watermark = INVALID_WATERMARK;
        bool muted = false;
        bool required_checkpoint = false;
    };

    SERDE HashJoinPtr join;

    Chunk output_header_chunk;
    size_t left_watermark_column_position;
    size_t right_watermark_column_position;
    Int64 latency_threshold;

    mutable std::mutex mutex;

    /// When received request checkpoint, it's always empty chunk with checkpoint context
    LeftInputPortWithData left_input;
    RightInputPortWithData right_input;

    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE std::list<Chunk> output_chunks;

    struct AlignmentStats
    {
        UInt64 left_input_muted = 0;
        UInt64 right_input_muted = 0;
    };
    AlignmentStats stats;

    Int64 last_stats_log_ts;
    Poco::Logger * log;
};
}
}
