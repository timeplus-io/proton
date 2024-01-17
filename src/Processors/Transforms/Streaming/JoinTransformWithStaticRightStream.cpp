#include <Processors/Transforms/Streaming/JoinTransformWithStaticRightStream.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
Block JoinTransformWithStaticRightStream::transformHeader(Block header, const HashJoinPtr & join)
{
    join->transformHeader(header);
    return header;
}

JoinTransformWithStaticRightStream::JoinTransformWithStaticRightStream(
    Block left_input_header, Block right_input_header, Block output_header, HashJoinPtr join_, UInt64 join_max_cached_bytes_)
    : IProcessor({left_input_header, right_input_header}, {output_header}, ProcessorID::StreamingJoinTransformWithStaticRightStreamID)
    , join(std::move(join_))
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , logger(&Poco::Logger::get("StreamingJoinTransformWithStaticRightStream"))
{
    assert(join);

    /// We know the finalized left header, output header etc, post init HashJoin
    join->postInit(left_input_header, output_header, join_max_cached_bytes_);

    assert(!join->rangeBidirectionalHashJoin());
    assert(!join->bidirectionalHashJoin());
}

IProcessor::Status JoinTransformWithStaticRightStream::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & input_port : inputs)
            input_port.close();

        return Status::Finished;
    }

    /// Do not disable inputs, so they can be executed in parallel.
    if (!output.canPush())
        return Status::PortFull;

    /// Push if we have data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    auto & left_input = inputs.front();
    auto & right_input = inputs.back();

    if (left_input.isFinished())
    {
        output.finish();
        right_input.close();
        return Status::Finished;
    }

    /// Filling right data until left input has data to join. Then we will close right input, which is a static hash table. It's behavior is like a `stream join table`
    if (!right_input.isFinished())
    {
        right_input.setNeeded();
        if (right_input.hasData())
        {
            filling_chunk = right_input.pull(true);
            required_consecutive_filling = filling_chunk.isRetractedData();
            has_input = true;
        }
    }

    left_input.setNeeded();
    if (!required_consecutive_filling && left_input.hasData())
    {
        if (!right_input.isFinished())
            right_input.close();

        join_chunk = left_input.pull(true);
        has_input = true;
    }

    return has_input ? Status::Ready : Status::NeedData;
}

void JoinTransformWithStaticRightStream::work()
{
    Int64 start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += filling_chunk.bytes() + join_chunk.bytes();

    {
        has_input = false;

        if (filling_chunk.hasRows())
            join->insertRightBlock(inputs.back().getHeader().cloneWithColumns(filling_chunk.detachColumns()));

        Block res;
        if (join_chunk.hasRows())
        {
            res = inputs.front().getHeader().cloneWithColumns(join_chunk.detachColumns());
            join->joinLeftBlock(res);
        }

        if (auto num_rows = res.rows(); num_rows)
        {
            output_chunk.setColumns(res.getColumns(), num_rows);
            has_output = true;
        }
        else
        {
            auto heartbeat_chunk = output_header_chunk.clone();
            output_chunk.swap(heartbeat_chunk);
            has_output = true;
        }

        /// Piggy-back watermark
        /// We only do this piggy-back once for the output chunk if there is
        if (join_chunk.hasWatermark())
            output_chunk.setWatermark(join_chunk.getWatermark());
        else if (auto requested_ckpt = join_chunk.getCheckpointContext(); requested_ckpt)
        {
            checkpoint(requested_ckpt);

            /// Propagate request checkpoint
            output_chunk.setCheckpointContext(std::move(requested_ckpt));
        }
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void JoinTransformWithStaticRightStream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) { join->serialize(wb); });
}

void JoinTransformWithStaticRightStream::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) { join->deserialize(rb); });
}

void JoinTransformWithStaticRightStream::onCancel()
{
    join->cancel();
}

}
}
