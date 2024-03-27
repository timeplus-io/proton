#pragma once

#include <Checkpoint/CheckpointContextFwd.h>
#include <Core/LightChunk.h>
#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <base/ClockUtils.h>
#include <Common/serde.h>
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

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    struct InputPortWithData
    {
        explicit InputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        void add(Chunk && chunk);

        bool hasCompleteChunks() const noexcept { return !input_chunks.empty() && !(input_chunks.size() == 1 && required_update_processing); }

        bool isFull() const noexcept { return need_buffer_data_to_align && hasCompleteChunks(); }

        void serialize(WriteBuffer & wb) const;
        void deserialize(ReadBuffer & rb);

        InputPort * input_port;

        /// Input state
        /// NOTE: Assume the input chunk is time-ordered
        SERDE std::list<LightChunkWithTimestamp> input_chunks;
        /// For join transform, we keep track watermark by itself
        SERDE Int64 watermark = INVALID_WATERMARK;
        NO_SERDE Int64 last_data_ts = 0;
        NO_SERDE CheckpointContextPtr ckpt_ctx = nullptr;
        NO_SERDE bool muted = false;
        NO_SERDE bool required_update_processing = false;

        /// Input description
        std::optional<size_t> watermark_column_position;
        DataTypePtr watermark_column_type;
        bool need_buffer_data_to_align;
    };

    Status prepareLeftInput();
    Status prepareRightInput();

    void processLeftInputData(LightChunk & chunk);
    void processRightInputData(LightChunk & chunk);

    bool isInputInQuiesce(const InputPortWithData & input_with_data) const noexcept
    {
        return DB::MonotonicMilliseconds::now() - input_with_data.last_data_ts >= quiesce_threshold_ms;
    }

    void muteLeftInput() noexcept
    {
        left_input.muted = true;
        ++stats.left_input_muted;
    }

    void muteRightInput() noexcept
    {
        right_input.muted = true;
        ++stats.right_input_muted;
    }

    static void unmuteInput(InputPortWithData & input_with_data) noexcept { input_with_data.muted = false; }

    void onCancel() override;

private:
    SERDE HashJoinPtr join;

    Chunk output_header_chunk;
    Int64 latency_threshold;
    Int64 quiesce_threshold_ms;

    SERDE InputPortWithData left_input;
    SERDE InputPortWithData right_input;
    bool need_propagate_heartbeat = false;

    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    NO_SERDE ChunkList output_chunks;

    struct AlignmentStats
    {
        UInt64 left_input_muted = 0;
        UInt64 right_input_muted = 0;
        UInt64 left_quiesce_joins = 0;
        UInt64 right_quiesce_joins = 0;
    };
    AlignmentStats stats;

    Int64 last_stats_log_ts;
    Poco::Logger * log;
};
}
