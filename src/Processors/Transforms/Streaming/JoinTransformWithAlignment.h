#pragma once

#include <Checkpoint/CheckpointContextFwd.h>
#include <Core/LightChunk.h>
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

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    struct InputPortWithData
    {
        explicit InputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;

        /// Input state
        /// NOTE: Assume the input chunk is time-ordered
        /// pair-<retracted_chunk, chunk>
        SERDE std::list<std::pair<LightChunk, LightChunkWithTimestamp>> input_chunks;
        /// For join transform, we keep track watermark by itself
        SERDE Int64 watermark = INVALID_WATERMARK;
        NO_SERDE Int64 last_data_ts = 0;
        NO_SERDE CheckpointContextPtr ckpt_ctx = nullptr;
        NO_SERDE bool muted = false;
        NO_SERDE bool required_update_processing = false;

        /// Input description
        std::optional<size_t> watermark_column_position;
        DataTypePtr watermark_column_type;

        inline void add(Chunk && chunk)
        {
            ckpt_ctx = chunk.getCheckpointContext();

            /// If the input needs to update data, currently the input is always two consecutive chunks with _tp_delta `-1 and +1`
            /// So we have to process them together before processing another input
            /// NOTE: Assume the first retracted chunk of updated data always set RetractedDataFlag.
            if (chunk.isRetractedData())
            {
                input_chunks.emplace_back(std::move(chunk), LightChunkWithTimestamp{});
                required_update_processing = true;
                return;
            }

            if (watermark_column_position)
            {
                if (likely(chunk.hasRows()))
                {
                    auto [min_ts, max_ts] = columnMinMaxTimestamp(chunk.getColumns()[*watermark_column_position], watermark_column_type);

                    if (required_update_processing)
                    {
                        input_chunks.back().second = LightChunkWithTimestamp{std::move(chunk), min_ts, max_ts};
                        required_update_processing = false;
                    }
                    else
                        input_chunks.emplace_back(LightChunk{}, LightChunkWithTimestamp{std::move(chunk), min_ts, max_ts});
                }
                else
                    input_chunks.emplace_back(LightChunk{}, LightChunkWithTimestamp{std::move(chunk), watermark, watermark});
            }
            else
            {
                auto now_ts = DB::UTCMilliseconds::now();
                if (required_update_processing)
                {
                    input_chunks.back().second = LightChunkWithTimestamp{std::move(chunk), now_ts, now_ts};
                    required_update_processing = false;
                }
                else
                    input_chunks.emplace_back(LightChunk{}, LightChunkWithTimestamp{std::move(chunk), now_ts, now_ts});
            }

            watermark = std::max(input_chunks.back().second.maxTimestamp(), watermark);
        }

        bool hasValidInputs() const noexcept { return !input_chunks.empty() && !(input_chunks.size() == 1 && required_update_processing); }

        Int64 minTimestamp() const noexcept { return hasValidInputs() ? input_chunks.front().second.minTimestamp() : watermark; }
    };

    Status prepareInput(InputPortWithData & input_with_data);

    template <bool is_left_block>
    void processInputData(LightChunk & chunk);

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

    void unmuteInput(InputPortWithData & input_with_data) noexcept { input_with_data.muted = false; }

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
    NO_SERDE std::list<Chunk> output_chunks;

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
