#include <Processors/Transforms/Streaming/JoinTransformWithAlignment.h>

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
    latency_threshold = left_join_stream_desc->latency_threshold;
    if (latency_threshold <= 0)
        latency_threshold = 200;

    auto right_join_stream_desc = join->rightJoinStreamDescription();

    left_watermark_column_position = left_join_stream_desc->lagBehindColumnPosition();
    left_watermark_column_type = left_join_stream_desc->lagBehindColumnType();

    right_watermark_column_position = right_join_stream_desc->lagBehindColumnPosition();
    right_watermark_column_type = right_join_stream_desc->lagBehindColumnType();

    assert(
        (left_watermark_column_position && right_watermark_column_position)
        || (!left_watermark_column_position && !right_watermark_column_position));

    assert((left_watermark_column_type && right_watermark_column_type) || (!left_watermark_column_type && !right_watermark_column_type));

    /// When left stream or right stream is in quiesce, we still need to push execute the join probably
    quiesce_threshold = std::max<Int64>(latency_threshold / 2, 500);

    LOG_INFO(
        log,
        "quiesce_threshold={} lag_latency_threshold={} left_lag_column={}, right_lag_column={}",
        quiesce_threshold,
        latency_threshold,
        left_join_stream_desc->lag_column,
        right_join_stream_desc->lag_column);
}

IProcessor::Status JoinTransformWithAlignment::prepareRightInput()
{
    if (right_input.input_chunk)
    {
        /// When we already pulled data from right input and didn't consume it yet,
        /// consume it.
        return Status::Ready;
    }
    else if (right_input.input_port->isFinished())
    {
        /// Close the other input port
        left_input.input_port->close();

        outputs.front().finish();

        return Status::Finished;
    }
    else if (!right_input.muted)
    {
        right_input.input_port->setNeeded();

        if (right_input.input_port->hasData())
        {
            right_input.input_chunk = right_input.input_port->pull(true);
            if (right_input.input_chunk.hasRows())
            {
                if (auto new_watermark = getRightWatermark(right_input.input_chunk); new_watermark > right_input.watermark)
                    right_input.watermark = new_watermark;

                right_input.last_data_ts = DB::MonotonicMilliseconds::now();
            }
            right_input.required_checkpoint = right_input.input_chunk.requestCheckpoint();
            return Status::Ready;
        }
    }

    return Status::NeedData;
}

IProcessor::Status JoinTransformWithAlignment::prepareLeftInput()
{
    if (!left_input.input_chunks.empty())
    {
        /// When we already pulled data from left input and didn't consume them yet,
        /// consume them.
        return Status::Ready;
    }
    else if (left_input.input_port->isFinished())
    {
        /// Close the other input port
        right_input.input_port->close();

        outputs.front().finish();

        return Status::Finished;
    }
    else if (!left_input.muted)
    {
        left_input.input_port->setNeeded();

        if (left_input.input_port->hasData())
        {
            left_input.input_chunks.push_back(left_input.input_port->pull(true));

            if (left_input.input_chunks.back().hasRows())
            {
                if (auto new_watermark = getLeftWatermark(left_input.input_chunks.back()); new_watermark > left_input.watermark)
                    left_input.watermark = new_watermark;

                left_input.last_data_ts = DB::MonotonicMilliseconds::now();
            }
            left_input.required_checkpoint = left_input.input_chunks.back().requestCheckpoint();
            return Status::Ready;
        }
    }

    return Status::NeedData;
}

IProcessor::Status JoinTransformWithAlignment::prepare()
{
    std::scoped_lock lock(mutex);

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
    assert(!left_input.muted || !right_input.muted);

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

/// Watermark alignment algorithm in general:
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
void JoinTransformWithAlignment::work()
{
    {
        std::scoped_lock lock(mutex);

        /// 1) We pulled right input and it has data
        if (right_input.input_chunk)
        {
            if (right_input.input_chunk.hasRows())
            {
                join->insertRightBlock(right_input.input_port->getHeader().cloneWithColumns(right_input.input_chunk.detachColumns()));
            }
            else
            {
                /// FIXME, watermark / heartbeat / checkpoint chunk.
                right_input.input_chunk.clear();
            }
        }

        /// Left input has buffered data or newly pulled data
        if (!left_input.input_chunks.empty())
        {
            auto right_input_in_quiesce = isRightInputInQuiesce();
            if ((left_input.watermark + latency_threshold <= right_input.watermark) || right_input_in_quiesce)
            {
                /// right input already progressed ahead enough or it is in quiesce, it's time to join
                for (auto & left_chunk : left_input.input_chunks)
                {
                    if (likely(left_chunk.hasRows()))
                    {
                        auto joined_block = left_input.input_port->getHeader().cloneWithColumns(left_chunk.detachColumns());
                        join->joinLeftBlock(joined_block);

                        if (auto rows = joined_block.rows(); rows > 0)
                            output_chunks.emplace_back(joined_block.getColumns(), rows);
                    }
                    else
                    {
                        /// FIXME, watermark / heartbeat / checkpoint chunk.
                    }
                }

                left_input.input_chunks.clear();

                /// Continue reading more data from left input
                left_input.muted = false;

                if (right_input_in_quiesce)
                    right_input.muted = false;
            }
            else
            {
                /// Left stream is leading ahead, mute left input to wait right input to progress
                left_input.muted = true;
                right_input.muted = false;
                stats.left_input_muted += 1;
            }
        }
        else
        {
            /// Left input doesn't have any data yet, we like to pull some. Mute the right input to
            /// give left input more chance to catch up.
            left_input.muted = false;

            /// If the right input doesn't have watermark yet, don't mute it
            if (likely(right_input.watermark != INVALID_WATERMARK))
            {
                right_input.muted = true;
                stats.right_input_muted += 1;
            }
        }
    }

    if (DB::MonotonicSeconds::now() - last_stats_log_ts >= 5)
    {
        LOG_INFO(
            log,
            "left_watermark={} right_watermark={} left_input_muted={} right_input_muted={}",
            left_input.watermark,
            right_input.watermark,
            stats.left_input_muted,
            stats.right_input_muted);

        last_stats_log_ts = DB::MonotonicSeconds::now();
    }
}

void JoinTransformWithAlignment::onCancel()
{
    join->cancel();
}
}
