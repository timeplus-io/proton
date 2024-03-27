#include <Processors/Transforms/Streaming/JoinTransformWithAlignment.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
Block JoinTransformWithAlignment::transformHeader(Block header, const HashJoinPtr & join)
{
    join->transformHeader(header);
    return header;
}

JoinTransformWithAlignment::JoinTransformWithAlignment(
    Block left_input_header, Block right_input_header, Block output_header, HashJoinPtr join_, UInt64 join_max_cached_bytes)
    : IProcessor({left_input_header, right_input_header}, {output_header}, ProcessorID::StreamingJoinTransformWithAlignmentID)
    , join(std::move(join_))
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , left_input{&inputs.front()}
    , right_input{&inputs.back()}
    , last_stats_log_ts(DB::MonotonicSeconds::now())
    , log(&Poco::Logger::get("StreamingJoinTransformWithAlignment"))
{
    assert(join);

    /// We know the finalized left header, output header etc, post init HashJoin
    join->postInit(left_input_header, output_header, join_max_cached_bytes);

    auto left_join_stream_desc = join->leftJoinStreamDescription();
    auto right_join_stream_desc = join->rightJoinStreamDescription();

    left_input.watermark_column_position = left_join_stream_desc->alignmentKeyColumnPosition();
    left_input.watermark_column_type = left_join_stream_desc->alignmentKeyColumnType();

    right_input.watermark_column_position = right_join_stream_desc->alignmentKeyColumnPosition();
    right_input.watermark_column_type = right_join_stream_desc->alignmentKeyColumnType();

    /// Some join combinations have builtin buffer to do aligning, so don't build an extra external buffer to align:
    /// 1) For asof join, the right side has builtin buffer to keep multiple versions
    /// 2) For range join, the left and right side both have builtin buffer to keep multiple ranges
    left_input.need_buffer_data_to_align = !join->leftStreamRequiresBufferingDataToAlign();
    right_input.need_buffer_data_to_align = !join->rightStreamRequiresBufferingDataToAlign();

    assert(
        (left_input.watermark_column_position && right_input.watermark_column_position)
        || (!left_input.watermark_column_position && !right_input.watermark_column_position));

    assert(
        (left_input.watermark_column_type && right_input.watermark_column_type)
        || (!left_input.watermark_column_type && !right_input.watermark_column_type));

    latency_threshold = left_join_stream_desc->latency_threshold;
    if (left_input.watermark_column_type)
    {
        if (!left_input.watermark_column_type->equals(*right_input.watermark_column_type)
            || !(isDateTime64(left_input.watermark_column_type) || isDateTime(left_input.watermark_column_type)))
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "The join alignement key type must be consistent datetime or datetime64, but got left={}({}) right={}({})",
                left_join_stream_desc->alignment_column,
                left_input.watermark_column_type->getName(),
                right_join_stream_desc->alignment_column,
                right_input.watermark_column_type->getName());

        /// Convert latency_threshold(ms) to latency_threshold(scale)
        UInt32 scale = 0;
        if (auto * data_type_datetime64 = checkAndGetDataType<DataTypeDateTime64>(left_input.watermark_column_type.get()))
            scale = data_type_datetime64->getScale();

        if (scale > 3)
            latency_threshold *= intExp10(3 - scale);
        else if (scale < 3)
            latency_threshold /= intExp10(scale - 3);
    }

    /// When left stream or right stream is in quiesce, we still need to push execute the join probably
    quiesce_threshold_ms = left_join_stream_desc->quiesce_threshold_ms;
    if (quiesce_threshold_ms <= 0)
        quiesce_threshold_ms = std::max<Int64>(std::abs(latency_threshold) / 2, 500);

    left_input.last_data_ts = right_input.last_data_ts = DB::MonotonicMilliseconds::now();

    LOG_INFO(
        log,
        "quiesce_threshold_ms={} lag_latency_threshold={} left_alignment_column={}, right_alignment_column={}",
        quiesce_threshold_ms,
        latency_threshold,
        left_join_stream_desc->alignment_column,
        right_join_stream_desc->alignment_column);
}

IProcessor::Status JoinTransformWithAlignment::prepareLeftInput()
{
    if (left_input.hasCompleteChunks())
    {
        /// In case, left input request checkpoint, then we need wait for the right input receive a checkpoint request
        if (left_input.ckpt_ctx)
            return Status::NeedData;

        /// When we already pulled data from left input and didn't consume them yet, consume them.
        /// A special case: if right input request checkpoint, we always expect to receive a checkpoint request ourselves
        if (likely(!right_input.ckpt_ctx))
            return Status::Ready;
    }

    if (left_input.input_port->isFinished())
    {
        /// Close the other input port
        right_input.input_port->close();
        return Status::Finished;
    }

    if (!left_input.muted)
    {
        left_input.input_port->setNeeded();
        if (left_input.input_port->hasData())
        {
            left_input.add(left_input.input_port->pull(true));
            need_propagate_heartbeat = true;
            return Status::Ready;
        }
    }

    return Status::NeedData;
}

IProcessor::Status JoinTransformWithAlignment::prepareRightInput()
{
    if (right_input.hasCompleteChunks())
    {
        /// In case, right input request checkpoint, then we need wait for the left input receive a checkpoint request
        if (right_input.ckpt_ctx)
            return Status::NeedData;

        /// When we already pulled data from left input and didn't consume them yet, consume them.
        /// A special case: if left input request checkpoint, we always expect to receive a checkpoint request ourselves
        if (likely(!left_input.ckpt_ctx))
            return Status::Ready;
    }

    if (right_input.input_port->isFinished())
    {
        /// Close the other input port
        left_input.input_port->close();
        return Status::Finished;
    }

    if (!right_input.muted)
    {
        right_input.input_port->setNeeded();
        if (right_input.input_port->hasData())
        {
            right_input.add(right_input.input_port->pull(true));
            need_propagate_heartbeat = true;
            return Status::Ready;
        }
    }

    return Status::NeedData;
}

IProcessor::Status JoinTransformWithAlignment::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        left_input.input_port->close();
        right_input.input_port->close();
        return Status::Finished;
    }

    /// Do not disable inputs, so they can be executed in parallel.
    if (!output.canPush())
        return Status::PortFull;

    /// Push if we have data.
    if (!output_chunks.empty())
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
        return Status::PortFull;
    }

    /// Invariant: at any specific time, we can't have both inputs muted
    assert(!right_input.muted || !left_input.muted);

    Status right_input_status = prepareRightInput();
    Status left_input_status = prepareLeftInput();

    if (right_input_status == Status::Ready || left_input_status == Status::Ready)
        /// One of the input still has buffered data, try to consume it
        /// The next round prepare, we will find all inputs are finished, then return Finished status
        return Status::Ready;
    else if (right_input_status == Status::Finished && left_input_status == Status::Finished)
        return Status::Finished;
    else
        return Status::NeedData;
}

/// FIXME: Separate enrichment join and biredictional join implementation
/// Watermark alignment algorithm in general:
/// 1) Pull the right stream first and track its watermark / timestamp as right_stream_watermark.
///     a. If events' right_stream_watermark <= left_stream_watermark + latency_threshold,
///         i) we can send the data to update right hash table (and join the left hash table for bidirectional join)
///         ii) Continue pulling more right stream data until the `right_stream_watermark < left_stream_watermark + latency_threshold`.
///            and buffer these events which hold `right_stream_watermark < left_stream_watermark + latency_threshold` events in JoinTransform for next join.
///
/// 2) If there are no buffered data for left stream. Pull the left stream next, and track its watermark / timestamp
///    as left_stream_watermark.
///    a. If events' left_stream_watermark + latency_threshold < right_stream_watermark,
///       i) we can send the data to join the right hash table.
///       ii) Continue pulling more left stream data until the `left_stream_watermark + latency_threshold >= right_stream_watermark`.
///            and buffer these events which hold `left_stream_watermark + latency_threshold >= right_stream_watermark` events in JoinTransform for next join.
///    b. If events' left_stream_watermark + latency_threshold >= right_stream_watermark, buffer these events in JoinTransform.
///       Mute left stream and unmute right stream if necessary
/// 3) If there are already buffered data for left stream (it shall be muted). Check if we can send the buffered to join the right hashtable and send as
///    much as possible. If the left buffered data is empty, unmute it.
///
/// If left input got stuck, basically we will need mute right input to avoid having too much memory pressure or wait for quiesce_threshold period.
/// If right input got stuck, we will need mute left input to avoid having too much memory pressure or wait for quiesce_threshold period.
/// After that, we continue the join which may join stale left/right data
/// but this is the best we can do since if we don't join, we stuck forever (this is a trade off, we can choose to stuck until right input
/// has more data, but in some scenario, they just didn't have more data).

void JoinTransformWithAlignment::work()
{
    bool left_input_in_quiesce = isInputInQuiesce(left_input);
    bool right_input_in_quiesce = isInputInQuiesce(right_input);

    /// Right input has buffered data or newly pulled data
    if (right_input.hasCompleteChunks())
    {
        /// We can directly process current input data in followsing scenarios:
        /// 1) Don't need align inputs (right side of join has builtin buffer to align)
        /// 2) Right stream's watermark less than or equal left stream's watermark + lantency_threshold
        /// 3) Or left input is in quiesce, we still need to push execute the join probably
        if (!right_input.need_buffer_data_to_align || right_input.watermark <= left_input.watermark + latency_threshold
            || left_input_in_quiesce)
        {
            do
            {
                if (isCancelled())
                    return;

                auto & chunk = right_input.input_chunks.front();
                assert(chunk.rows());
                processRightInputData(chunk.chunk);

                right_input.input_chunks.pop_front();
            } while (right_input.hasCompleteChunks());

            if (left_input_in_quiesce)
            {
                unmuteInput(left_input);
                ++stats.left_quiesce_joins;
            }
        }
        else
            unmuteInput(left_input);
    }

    /// Left input has buffered data or newly pulled data
    if (left_input.hasCompleteChunks())
    {
        /// We can directly process current input data in followsing scenarios:
        /// 1) Don't need align inputs (left side of join has builtin buffer to align)
        /// 2) Left stream's watermark + latency_threshold less than right stream's watermark
        /// 3) Or right input is in quiesce, we still need to push execute the join probably
        if (!left_input.need_buffer_data_to_align || left_input.watermark + latency_threshold < right_input.watermark
            || right_input_in_quiesce)
        {
            do
            {
                if (isCancelled())
                    return;

                auto & chunk = left_input.input_chunks.front();
                assert(chunk.rows());
                processLeftInputData(chunk.chunk);

                left_input.input_chunks.pop_front();
            } while (left_input.hasCompleteChunks());

            if (right_input_in_quiesce)
            {
                unmuteInput(right_input);
                ++stats.right_quiesce_joins;
            }
        }
        else
            unmuteInput(right_input);
    }

    if (!left_input.hasCompleteChunks())
    {
        /// Left input has not valid chunks, unmute it
        unmuteInput(left_input);
        if (right_input.isFull())
            muteRightInput();
    }

    if (!right_input.hasCompleteChunks())
    {
        /// Right input has not valid chunks, unmute it.
        unmuteInput(right_input);
        if (left_input.isFull())
            muteLeftInput();
    }

    if (left_input.ckpt_ctx && right_input.ckpt_ctx)
    {
        checkpoint(right_input.ckpt_ctx);

        /// Propagate request checkpoint
        output_chunks.emplace_back(output_header_chunk.clone());
        output_chunks.back().setCheckpointContext(std::move(right_input.ckpt_ctx));

        unmuteInput(left_input);
        unmuteInput(right_input);
        left_input.ckpt_ctx = right_input.ckpt_ctx = nullptr;
    }
    else if (left_input.ckpt_ctx)
    {
        muteLeftInput(); /// mute left input to until right input received checkpoint request
        unmuteInput(right_input);
    }
    else if (right_input.ckpt_ctx)
    {
        muteRightInput(); /// mute right input to until left input received checkpoint request
        unmuteInput(left_input);
    }

    /// We propagate empty chunk without watermark.
    if (need_propagate_heartbeat && output_chunks.empty())
        output_chunks.emplace_back(output_header_chunk.clone());

    need_propagate_heartbeat = false;

    if (DB::MonotonicSeconds::now() - last_stats_log_ts >= 60)
    {
        LOG_INFO(
            log,
            "{}, left_watermark={} right_watermark={} left_input_muted={} right_input_muted={} left_quiesce_joins={} right_quiesce_joins={}",
            join->metricsString(),
            left_input.watermark,
            right_input.watermark,
            stats.left_input_muted,
            stats.right_input_muted,
            stats.left_quiesce_joins,
            stats.right_quiesce_joins);

        last_stats_log_ts = DB::MonotonicSeconds::now();
    }
}

void JoinTransformWithAlignment::processLeftInputData(LightChunk & chunk)
{
    /// FIXME: Provide a unified interface for different joins.
    if (join->rangeBidirectionalHashJoin())
    {
        auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        auto joined_blocks = join->insertLeftBlockToRangeBucketsAndJoin(block);
        for (size_t j = 0; j < joined_blocks.size(); ++j)
            output_chunks.emplace_back(joined_blocks[j].getColumns(), joined_blocks[j].rows());
    }
    else if (join->bidirectionalHashJoin())
    {
        auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        auto retracted_block = join->insertLeftBlockAndJoin(block);
        if (auto retracted_block_rows = retracted_block.rows(); retracted_block_rows > 0)
        {
            /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setConsecutiveDataFlag();
            output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
        }

        if (block.rows())
            output_chunks.emplace_back(block.getColumns(), block.rows());
    }
    else
    {
        auto joined_block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        join->joinLeftBlock(joined_block);

        if (auto rows = joined_block.rows(); rows > 0)
            output_chunks.emplace_back(joined_block.getColumns(), rows);
    }
}

void JoinTransformWithAlignment::processRightInputData(LightChunk & chunk)
{
    /// FIXME: Provide a unified interface for different joins.
    if (join->rangeBidirectionalHashJoin())
    {
        auto block = inputs.back().getHeader().cloneWithColumns(chunk.detachColumns());
        auto joined_blocks = join->insertRightBlockToRangeBucketsAndJoin(block);
        for (size_t j = 0; j < joined_blocks.size(); ++j)
            output_chunks.emplace_back(joined_blocks[j].getColumns(), joined_blocks[j].rows());
    }
    else if (join->bidirectionalHashJoin())
    {
        auto block = inputs.back().getHeader().cloneWithColumns(chunk.detachColumns());
        auto retracted_block = join->insertRightBlockAndJoin(block);
        if (auto retracted_block_rows = retracted_block.rows(); retracted_block_rows > 0)
        {
            /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setConsecutiveDataFlag();
            output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
        }

        if (block.rows())
            output_chunks.emplace_back(block.getColumns(), block.rows());
    }
    else
    {
        join->insertRightBlock(inputs.back().getHeader().cloneWithColumns(chunk.detachColumns()));
    }
}

void JoinTransformWithAlignment::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        /// Serializing join algorithm state
        join->serialize(wb, getVersion());

        /// Serializing left_input state
        left_input.serialize(wb);

        /// Serializing right_input state
        right_input.serialize(wb);
    });
}

void JoinTransformWithAlignment::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType version_, ReadBuffer & rb) {
        /// Deserializing join algorithm state
        join->deserialize(rb, version_);

        /// Deserializing left_input state
        left_input.deserialize(rb);

        /// Deserializing right_input state
        right_input.deserialize(rb);
    });

    /// Re-init last data ts
    left_input.last_data_ts = right_input.last_data_ts = DB::MonotonicMilliseconds::now();
}

void JoinTransformWithAlignment::onCancel()
{
    join->cancel();
}


void JoinTransformWithAlignment::InputPortWithData::add(Chunk && chunk)
{
    ckpt_ctx = chunk.getCheckpointContext();

    /// If the input needs to update data, currently the input is always two consecutive chunks with _tp_delta `-1 and +1`
    /// So we have to process them together before processing another input
    /// NOTE: Assume the first retracted chunk of updated data always set RetractedDataFlag.
    if (chunk.isConsecutiveData())
    {
        assert(chunk.hasRows());
        last_data_ts = DB::MonotonicMilliseconds::now();
        input_chunks.emplace_back(std::move(chunk), watermark, watermark);
        required_update_processing = true;
        return;
    }
    else if (required_update_processing)
        required_update_processing = false;

    if (likely(chunk.hasRows()))
    {
        if (watermark_column_position)
        {
            auto [min_ts, max_ts] = columnMinMaxTimestamp(chunk.getColumns()[*watermark_column_position], watermark_column_type);
            input_chunks.emplace_back(std::move(chunk), min_ts, max_ts);
            watermark = std::max(max_ts, watermark);
        }
        else
        {
            auto now_ts = DB::UTCMilliseconds::now();
            input_chunks.emplace_back(std::move(chunk), now_ts, now_ts);
            watermark = now_ts;
        }

        last_data_ts = DB::MonotonicMilliseconds::now();
    }
}

void JoinTransformWithAlignment::InputPortWithData::serialize(WriteBuffer & wb) const
{
    if (need_buffer_data_to_align)
    {
        DB::writeVarUInt(input_chunks.size(), wb);
        const auto & header = input_port->getHeader();
        for (const auto & chunk : input_chunks)
            DB::writeLightChunkWithTimestamp(chunk, header, ProtonRevision::getVersionRevision(), wb);
    }
    else
    {
        /// Don't buffer input chunks and directly push to hash table, so we can assume no buffered data when received request checkpoint, ,
        assert(input_chunks.empty());
    }

    if (watermark_column_position)
        DB::writeIntBinary(watermark, wb);
}

void JoinTransformWithAlignment::InputPortWithData::deserialize(ReadBuffer & rb)
{
    if (need_buffer_data_to_align)
    {
        size_t size;
        DB::readVarUInt(size, rb);
        input_chunks.resize(size);
        const auto & header = input_port->getHeader();
        for (auto & chunk : input_chunks)
            chunk = DB::readLightChunkWithTimestamp(header, ProtonRevision::getVersionRevision(), rb);
    }

    if (watermark_column_position)
        DB::readIntBinary(watermark, rb);
}
}
}
