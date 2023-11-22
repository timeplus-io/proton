#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <base/ClockUtils.h>
#include <base/SerdeTag.h>
#include <Common/ColumnUtils.h>

namespace DB::Streaming
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
        Block left_input_header, Block right_input_header, Block output_header, HashJoinPtr join_, UInt64 join_max_cached_bytes_);

    String getName() const override { return "StreamingJoinTransformWithAlignment"; }
    Status prepare() override;
    void work() override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    Status prepareLeftInput();
    Status prepareRightInput();

    inline Int64 getRightWatermark(const Chunk & chunk) const
    {
        return getWatermark(chunk, right_watermark_column_position, right_watermark_column_type);
    }

    inline Int64 getLeftWatermark(const Chunk & chunk) const
    {
        return getWatermark(chunk, left_watermark_column_position, left_watermark_column_type);
    }

    inline Int64 getWatermark(const Chunk & chunk, std::optional<size_t> col_pos, const DataTypePtr & col_type) const
    {
        if (col_pos)
        {
            auto [_, max_ts] = columnMinMaxTimestamp(chunk.getColumns()[*col_pos], col_type);
            return max_ts;
        }
        else
            return DB::UTCMilliseconds::now();
    }

    bool isRightInputInQuiesce() const noexcept { return DB::MonotonicMilliseconds::now() - right_input.last_data_ts >= quiesce_threshold; }

    void onCancel() override;

private:
    struct LeftInputPortWithData
    {
        explicit LeftInputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        std::list<Chunk> input_chunks;

        /// For join transform, we keep track watermark by itself
        Int64 watermark = INVALID_WATERMARK;
        Int64 last_data_ts = 0;
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
        Int64 last_data_ts = 0;
        bool muted = false;
        bool required_checkpoint = false;
    };

    SERDE HashJoinPtr join;

    Chunk output_header_chunk;
    std::optional<size_t> left_watermark_column_position;
    std::optional<size_t> right_watermark_column_position;
    DataTypePtr left_watermark_column_type;
    DataTypePtr right_watermark_column_type;
    Int64 latency_threshold;
    Int64 quiesce_threshold;

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
