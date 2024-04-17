#include <Processors/Transforms/Streaming/JoinTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/joinKind.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace Streaming
{
Block JoinTransform::transformHeader(Block header, const HashJoinPtr & join)
{
    join->transformHeader(header);
    return header;
}

JoinTransform::JoinTransform(
    Block left_input_header,
    Block right_input_header,
    Block output_header,
    HashJoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_cached_bytes)
    : IProcessor({left_input_header, right_input_header}, {output_header}, ProcessorID::StreamingJoinTransformID)
    , join(std::move(join_))
    , max_block_size(max_block_size_)
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , logger(&Poco::Logger::get("StreamingJoinTransform"))
    , input_ports_with_data{InputPortWithData{&inputs.front()}, InputPortWithData{&inputs.back()}}
    , last_log_ts(MonotonicSeconds::now())
{
    assert(join);

    /// We know the finalized left header, output header etc, post init HashJoin
    join->postInit(left_input_header, output_header, join_max_cached_bytes);

    range_bidirectional_hash_join = join->rangeBidirectionalHashJoin();
    bidirectional_hash_join = join->bidirectionalHashJoin();
}

IProcessor::Status JoinTransform::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & port_ctx : input_ports_with_data)
            port_ctx.input_port->close();

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

    Status status = Status::NeedData;

    for (size_t i = 0; auto & input_port_with_data : input_ports_with_data)
    {
        if (input_port_with_data.input_chunk)
        {
            /// In case, this input port request checkpoint, so we need wait for other inputs
            if (input_port_with_data.input_chunk.requestCheckpoint())
                continue;

            /// In case, this input need wait for another input processing next consecutive chunk done.
            if (required_update_processing_index.has_value() && *required_update_processing_index != i)
                continue;

            status = Status::Ready;
        }
        else if (input_port_with_data.input_port->isFinished())
        {
            output.finish();
            /// Close the other input port
            input_ports_with_data[(i + 1) % input_ports_with_data.size()].input_port->close();
            /// We like to execute last input chunk from the other input
            /// The next round prepare, we will find all inputs are finished, then return Finished status
            if (status != Status::Ready)
            {
                status = Status::Finished;
                break;
            }
        }
        else
        {
            input_port_with_data.input_port->setNeeded();

            if (input_port_with_data.input_port->hasData())
            {
                input_port_with_data.input_chunk = input_port_with_data.input_port->pull(true);
                status = Status::Ready;
            }
        }
        ++i;
    }

    return status;
}

void JoinTransform::work()
{
    int64_t local_watermark = std::numeric_limits<int64_t>::max();

    bool has_watermark = false;
    bool has_data = false;
    UInt8 requested_checkpoint_num = 0;
    CheckpointContextPtr requested_ckpt;

    Chunks chunks;
    {
        /// Move out the input chunks
        assert(input_ports_with_data[0].input_chunk || input_ports_with_data[1].input_chunk);

        for (size_t i = 0; i < input_ports_with_data.size(); ++i)
        {
            auto & input_chunk = input_ports_with_data[i].input_chunk;
            if (input_chunk)
            {
                /// If any input needs to update data, currently the input is always two consecutive chunks with _tp_delta `-1 and +1`
                /// So we have to process them together before processing another input
                /// NOTE: Assume the first retracted chunk of updated data always set RetractedDataFlag.
                if (required_update_processing_index.has_value())
                {
                    if (*required_update_processing_index != i)
                        continue;

                    required_update_processing_index.reset();
                }
                else if (input_chunk.isConsecutiveData())
                    required_update_processing_index = i;

                if (input_chunk.hasWatermark())
                {
                    auto input_chunk_watermark = input_chunk.getChunkContext()->getWatermark();

                    local_watermark = std::min(local_watermark, input_chunk_watermark);
                    has_watermark = true;
                }
                else if (input_chunk.requestCheckpoint())
                {
                    ++requested_checkpoint_num;
                    continue; /// keep in input_ports_with_data until all inputs checkpoint requested
                }

                if (input_chunk.hasRows())
                    has_data = true;

                chunks[i].swap(input_chunk);
            }
        }

        /// We propagate empty chunk with or without watermark.
        /// Skip propagate if needs to processing next consecutive chunk
        /// to avoid downstream aggregation to emit transitive results we don't want
        if (!has_data && !required_update_processing_index)
            output_chunks.emplace_back(output_header_chunk.clone());

        /// All inputs request checkpoint
        if (requested_checkpoint_num == input_ports_with_data.size())
        {
            requested_ckpt = input_ports_with_data.front().input_chunk.getCheckpointContext();
            std::ranges::for_each(input_ports_with_data, [](auto & data) { data.input_chunk.clear(); });
        }
    }

    if (has_data)
        doJoin(std::move(chunks));

    /// Piggy-back watermark
    /// We only do this piggy-back once for the last output chunk if there is
    if (has_watermark)
    {
        if (!output_chunks.empty())
            setupWatermark(output_chunks.back(), local_watermark);
        else
            /// If there is no join result or chunks don't have data but have watermark, we still need propagate the watermark
            propagateWatermark(local_watermark);
    }
    else if (requested_ckpt)
    {
        checkpoint(requested_ckpt);

        /// Propagate request checkpoint
        assert(!output_chunks.empty());
        output_chunks.back().setCheckpointContext(std::move(requested_ckpt));
    }

    if (MonotonicSeconds::now() - last_log_ts > 60)
    {
        LOG_INFO(logger, "{}, watermark={}", join->metricsString(), watermark);
        last_log_ts = MonotonicSeconds::now();
    }
}

inline void JoinTransform::propagateWatermark(int64_t local_watermark)
{
    auto chunk = output_header_chunk.clone();
    if (setupWatermark(chunk, local_watermark))
        output_chunks.emplace_back(std::move(chunk));
}

inline bool JoinTransform::setupWatermark(Chunk & chunk, int64_t local_watermark)
{
    /// Watermark shall never regress
    if (local_watermark > watermark)
    {
        watermark = local_watermark;

        /// Propagate it
        chunk.setWatermark(local_watermark);
        return true;
    }
    return false;
}

inline void JoinTransform::doJoin(Chunks chunks)
{
    if (range_bidirectional_hash_join)
    {
        rangeJoinBidirectionally(std::move(chunks));
    }
    else if (bidirectional_hash_join)
    {
        joinBidirectionally(std::move(chunks));
    }
    else
    {
        /// First insert right block to update the build-side hash table
        if (chunks[1].hasRows())
            join->insertRightBlock(input_ports_with_data[1].input_port->getHeader().cloneWithColumns(chunks[1].detachColumns()));

        /// Then use left block to join the right updated hash table
        /// Please note in this mode, right stream data only changes won't trigger join since left stream data is not buffered
        if (chunks[0].hasRows())
        {
            auto joined_block = input_ports_with_data[0].input_port->getHeader().cloneWithColumns(chunks[0].detachColumns());
            join->joinLeftBlock(joined_block);

            if (auto rows = joined_block.rows(); rows > 0)
                output_chunks.emplace_back(joined_block.getColumns(), rows);
        }
    }
}

inline void JoinTransform::joinBidirectionally(Chunks chunks)
{
    std::array<decltype(&Streaming::IHashJoin::insertLeftBlockAndJoin), 2> join_funcs
        = {&Streaming::IHashJoin::insertLeftBlockAndJoin, &Streaming::IHashJoin::insertRightBlockAndJoin};

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        if (!chunks[i].hasRows())
            continue;

        auto block = input_ports_with_data[i].input_port->getHeader().cloneWithColumns(chunks[i].detachColumns());
        auto retracted_block = std::invoke(join_funcs[i], join.get(), block);

        /// First emit retracted block
        auto retracted_block_rows = retracted_block.rows();
        if (retracted_block_rows)
        {
            /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setConsecutiveDataFlag();
            output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
        }

        if (block.rows())
            output_chunks.emplace_back(block.getColumns(), block.rows());
    }
}

inline void JoinTransform::rangeJoinBidirectionally(Chunks chunks)
{
    std::array<decltype(&Streaming::IHashJoin::insertLeftBlockToRangeBucketsAndJoin), 2> join_funcs
        = {&Streaming::IHashJoin::insertLeftBlockToRangeBucketsAndJoin, &Streaming::IHashJoin::insertRightBlockToRangeBucketsAndJoin};

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        if (!chunks[i].hasRows())
            continue;

        auto block = input_ports_with_data[i].input_port->getHeader().cloneWithColumns(chunks[i].detachColumns());
        auto joined_blocks = std::invoke(join_funcs[i], join.get(), block);

        for (auto & joined_block : joined_blocks)
        {
            if (joined_block.rows())
                output_chunks.emplace_back(joined_block.getColumns(), joined_block.rows());
        }
    }
}

void JoinTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        join->serialize(wb, getVersion());
        DB::writeIntBinary(watermark, wb);
    });
}

void JoinTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType version_, ReadBuffer & rb) {
        join->deserialize(rb, version_);
        DB::readIntBinary(watermark, rb);
    });
}

void JoinTransform::onCancel()
{
    join->cancel();
}

}
}
