#include <Processors/Transforms/Streaming/JoinTransformWithAlignment.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>
#include <Common/logger_useful.h>

namespace DB::Streaming
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
    assert(join->requireWatermarkAlignedStreams());

    auto left_join_stream_desc = join->leftJoinStreamDescription();
    auto right_join_stream_desc = join->rightJoinStreamDescription();

    left_input.watermark_column_position = left_join_stream_desc->timestampColumnPosition();
    left_input.watermark_column_type = left_join_stream_desc->timestampColumnType();

    right_input.watermark_column_position = right_join_stream_desc->timestampColumnPosition();
    right_input.watermark_column_type = right_join_stream_desc->timestampColumnType();

    assert(
        (left_input.watermark_column_position && right_input.watermark_column_position)
        || (!left_input.watermark_column_position && !right_input.watermark_column_position));

    assert(
        (left_input.watermark_column_type && right_input.watermark_column_type)
        || (!left_input.watermark_column_type && !right_input.watermark_column_type));

    /// FIXME: Check and convert to same scale for aligned timestamp col type and latency_threshol(/ms), 
    latency_threshold = left_join_stream_desc->latency_threshold;

    /// When left stream or right stream is in quiesce, we still need to push execute the join probably
    quiesce_threshold_ms = left_join_stream_desc->quiesce_threshold;
    if (quiesce_threshold_ms <= 0)
        quiesce_threshold_ms = std::max<Int64>(std::abs(latency_threshold) / 2, 500);

    left_input.last_data_ts = right_input.last_data_ts = DB::MonotonicMilliseconds::now();

    LOG_INFO(
        log,
        "quiesce_threshold_ms={} lag_latency_threshold={} left_timestamp_column={}, right_timestamp_column={}",
        quiesce_threshold_ms,
        latency_threshold,
        left_join_stream_desc->timestamp_column,
        right_join_stream_desc->timestamp_column);
}

IProcessor::Status JoinTransformWithAlignment::prepareInput(InputPortWithData & input_with_data)
{
    if (input_with_data.hasValidInputs())
    {
        /// In case, this input port request checkpoint, so we need wait for other inputs
        if (input_with_data.ckpt_ctx)
            return Status::NeedData;

        /// When we already pulled data from left input and didn't consume them yet,
        /// consume them.
        return Status::Ready;
    }
    else if (input_with_data.input_port->isFinished())
    {
        /// Close the other input port
        right_input.input_port->close();
        right_input.input_port->close();
        return Status::Finished;
    }
    else if (!input_with_data.muted)
    {
        input_with_data.input_port->setNeeded();

        if (input_with_data.input_port->hasData())
        {
            auto chunk = input_with_data.input_port->pull(true);
            if (chunk.hasRows())
                input_with_data.last_data_ts = DB::MonotonicMilliseconds::now();

            input_with_data.add(std::move(chunk));
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

    Status right_input_status = prepareInput(right_input);
    Status left_input_status = prepareInput(left_input);

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
#if 0
/// Watermark alignment algorithm for enrichment join:
/// 1) Pull the right stream first and track its watermark / timestamp as right_stream_watermark.
///    Feed the data pulled to the right hash table directly which means JoinTransform doesn't buffer
///    right stream data.
/// 2) If there are no buffered data for left stream. Pull the left stream next, and track its watermark / timestamp
///    as left_stream_watermark.
///    a. If events' left_stream_watermark + latency_threshold <= right_stream_watermark,
///       i) we can send the data to join the right hash table.
///       ii) Mute right stream to avoid pulling more data since we may more left data which can be joined with right hash table.
///       iii) Continue pulling more left stream data until the `left_stream_watermark + latency_threshold > right_stream_watermark`.
///            and buffer these events which hold `left_stream_watermark + latency_threshold > right_stream_watermark` events in JoinTransform for next join.
///    b. If events' left_stream_watermark + latency_threshold > right_stream_watermark, buffer these events in JoinTransform.
///       Mute left stream and unmute right stream if necessary
/// 3) If there are already buffered data for left stream (it shall be muted). Check if we can send the buffered to join the right hashtable and send as
///    much as possible. If the left buffered data is empty, unmute it.
///
/// If left input got stuck, basically we will need mute right input to avoid having too much memory pressure.
/// If right input got stuck, we wait for quiesce_threshold period, after that, we continue the join which may join stale right data
/// but this is the best we can do since if we don't join, we stuck forever (this is a trade off, we can choose to stuck until right input
/// has more data, but in some scenario, they just didn't have more data).
///
/// When right stream can garbage collect its data ?
/// For asof join, for now, we manually use keep_versions to do garbage collection. Ideally we can do better job (automatically), but seems hard since version
/// and watermark / timestamp are different.
#endif

/// Watermark alignment algorithm in general:
/// 1) Pull the right stream first and track its watermark / timestamp as right_stream_watermark.
///     a. If events' `right_stream_watermark <= `,
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
    do
    {
        /// Right input has buffered data or newly pulled data
        while (right_input.hasValidInputs())
        {
            if (isCancelled())
                return;

            auto & [retracted_chunk, chunk] = right_input.input_chunks.front();
            if (chunk.maxTimestamp() <= left_input.minTimestamp() + latency_threshold || left_input_in_quiesce)
            {
                if (retracted_chunk.rows())
                    processInputData<false>(retracted_chunk);

                if (likely(chunk.rows()))
                    processInputData<false>(chunk.chunk);

                right_input.input_chunks.pop_front();

                if (left_input_in_quiesce)
                    unmuteInput(left_input);
                
                stats.left_quiesce_joins = left_input_in_quiesce;
            }
            else
            {
                unmuteInput(left_input);
                break;
            }
        }

        /// Left input has buffered data or newly pulled data
        while (left_input.hasValidInputs())
        {
            if (isCancelled())
                return;

            auto & [retracted_chunk, chunk] = left_input.input_chunks.front();
            if (chunk.maxTimestamp() + latency_threshold < right_input.minTimestamp() || right_input_in_quiesce)
            {
                if (retracted_chunk.rows())
                    processInputData<true>(retracted_chunk);

                if (likely(chunk.rows()))
                    processInputData<true>(chunk.chunk);

                left_input.input_chunks.pop_front();

                if (right_input_in_quiesce)
                    unmuteInput(right_input);
                
                stats.right_quiesce_joins += right_input_in_quiesce;
            }
            else
            {
                unmuteInput(right_input);
                break;
            }
        }
    } while (left_input.hasValidInputs() && right_input.hasValidInputs());

    if (!left_input.hasValidInputs())
    {
        /// Left input has not valid chunks, unmute it
        unmuteInput(left_input);
        /// If right input has at least one valid chunk (another one may be an incomplete updated chunk pair <retracted_chunk, null>),
        /// mute right input to avoid buffer too much data.
        /// FIXME: allow buffer more ? limited by rows or bytes.
        if (right_input.input_chunks.size() >= 2)
            muteRightInput();
    }

    if (!right_input.hasValidInputs())
    {
        /// Right input has not valid chunks, unmute it.
        unmuteInput(right_input);
        /// If left input has at least one valid chunk (another one may be an incomplete updated chunk pair <retracted_chunk, null>),
        /// mute left input to avoid buffer too much data.
        /// FIXME: allow buffer more ? limited by rows or bytes.
        if (left_input.input_chunks.size() >= 2)
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
        muteLeftInput(); /// mute lagging input to until leading input received checkpoint request
    else if (right_input.ckpt_ctx)
        muteRightInput(); /// mute leading input to until lagging input received checkpoint request

    /// We propagate empty chunk without watermark.
    if (need_propagate_heartbeat && output_chunks.empty())
        output_chunks.emplace_back(output_header_chunk.clone());

    need_propagate_heartbeat = false;

    if (DB::MonotonicSeconds::now() - last_stats_log_ts >= 5)
    {
        LOG_INFO(
            log,
            "left_watermark={} right_watermark={} left_input_muted={} right_input_muted={} left_quiesce_joins={} right_quiesce_joins={}",
            left_input.watermark,
            right_input.watermark,
            stats.left_input_muted,
            stats.right_input_muted,
            stats.left_quiesce_joins,
            stats.right_quiesce_joins);

        last_stats_log_ts = DB::MonotonicSeconds::now();
    }
}

template <bool is_left_input>
void JoinTransformWithAlignment::processInputData(LightChunk & chunk)
{
    /// FIXME: Provide a unified interface for different connections.
    if (join->rangeBidirectionalHashJoin())
    {
        Block block;
        Blocks joined_blocks;
        if constexpr (is_left_input)
        {
            block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
            joined_blocks = join->insertLeftBlockToRangeBucketsAndJoin(block);
        }
        else
        {
            block = inputs.back().getHeader().cloneWithColumns(chunk.detachColumns());
            joined_blocks = join->insertLeftBlockToRangeBucketsAndJoin(block);
        }

        for (size_t j = 0; j < joined_blocks.size(); ++j)
            output_chunks.emplace_back(joined_blocks[j].getColumns(), joined_blocks[j].rows());
    }
    else if (join->bidirectionalHashJoin())
    {
        Block block;
        Block retracted_block;
        if constexpr (is_left_input)
        {
            block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
            retracted_block = join->insertLeftBlockAndJoin(block);
        }
        else
        {
            block = inputs.back().getHeader().cloneWithColumns(chunk.detachColumns());
            retracted_block = join->insertRightBlockAndJoin(block);
        }

        auto retracted_block_rows = retracted_block.rows();
        if (retracted_block_rows)
        {
            /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setRetractedDataFlag();
            output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
        }

        if (block.rows())
            output_chunks.emplace_back(block.getColumns(), block.rows());
    }
    else
    {
        if constexpr (is_left_input)
        {
            auto joined_block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
            join->joinLeftBlock(joined_block);

            if (auto rows = joined_block.rows(); rows > 0)
                output_chunks.emplace_back(joined_block.getColumns(), rows);
        }
        else
            join->insertRightBlock(inputs.back().getHeader().cloneWithColumns(chunk.detachColumns()));
    }
}

void JoinTransformWithAlignment::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        join->serialize(wb);

        /// Serializing left_input
        assert(!left_input.required_update_processing);
        DB::writeVarUInt(left_input.input_chunks.size(), wb);
        const auto & left_header = left_input.input_port->getHeader();
        for (const auto & [retracted_chunk, chunk] : left_input.input_chunks)
        {
            bool has_retracted_data = static_cast<bool>(retracted_chunk);
            DB::writeIntBinary<Int8>(has_retracted_data, wb);
            if (has_retracted_data)
                DB::writeLightChunk(retracted_chunk, left_header, ProtonRevision::getVersionRevision(), wb);

            DB::writeLightChunkWithTimestamp(chunk, left_header, ProtonRevision::getVersionRevision(), wb);
        }

        if (left_input.watermark_column_position)
            DB::writeIntBinary(left_input.watermark, wb);

        /// Serializing right_input
        assert(!right_input.required_update_processing);
        DB::writeVarUInt(right_input.input_chunks.size(), wb);
        const auto & right_header = right_input.input_port->getHeader();
        for (const auto & [retracted_chunk, chunk] : right_input.input_chunks)
        {
            UInt8 has_retracted_data = retracted_chunk ? 1 : 0;
            DB::writeIntBinary<UInt8>(has_retracted_data, wb);
            if (has_retracted_data)
                DB::writeLightChunk(retracted_chunk, right_header, ProtonRevision::getVersionRevision(), wb);

            DB::writeLightChunkWithTimestamp(chunk, right_header, ProtonRevision::getVersionRevision(), wb);
        }

        if (right_input.watermark_column_position)
            DB::writeIntBinary(right_input.watermark, wb);
    });
}

void JoinTransformWithAlignment::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
        join->deserialize(rb);

        /// Deserializing left_input state
        {
            size_t size;
            DB::readVarUInt(size, rb);
            left_input.input_chunks.resize(size);
            const auto & left_header = left_input.input_port->getHeader();
            for (auto & [retracted_chunk, chunk] : left_input.input_chunks)
            {
                UInt8 has_retracted_data = 0;
                DB::readIntBinary<UInt8>(has_retracted_data, rb);
                if (has_retracted_data)
                    retracted_chunk = DB::readLightChunk(left_header, ProtonRevision::getVersionRevision(), rb);

                chunk = DB::readLightChunkWithTimestamp(left_header, ProtonRevision::getVersionRevision(), rb);
            }

            if (left_input.watermark_column_position)
                DB::readIntBinary(left_input.watermark, rb);
        }

        /// Deserializing right_input state
        {
            size_t size;
            DB::readVarUInt(size, rb);
            right_input.input_chunks.resize(size);
            const auto & right_header = right_input.input_port->getHeader();
            for (auto & [retracted_chunk, chunk] : right_input.input_chunks)
            {
                UInt8 has_retracted_data = 0;
                DB::readIntBinary<UInt8>(has_retracted_data, rb);
                if (has_retracted_data)
                    retracted_chunk = DB::readLightChunk(right_header, ProtonRevision::getVersionRevision(), rb);

                chunk = DB::readLightChunkWithTimestamp(right_header, ProtonRevision::getVersionRevision(), rb);
            }

            if (right_input.watermark_column_position)
                DB::readIntBinary(right_input.watermark, rb);
        }
    });
}

void JoinTransformWithAlignment::onCancel()
{
    join->cancel();
}
}
