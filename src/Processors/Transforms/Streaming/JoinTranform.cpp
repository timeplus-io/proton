#include <Processors/Transforms/Streaming/JoinTranform.h>

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
{
    assert(join);

    /// We know the finalized left header, output header etc, post init HashJoin
    join->postInit(left_input_header, output_header, join_max_cached_bytes);

    range_bidirectional_hash_join = join->rangeBidirectionalHashJoin();
    bidirectional_hash_join = join->bidirectionalHashJoin();
}

IProcessor::Status JoinTransform::prepare()
{
    std::scoped_lock lock(mutex);

    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        output.finish();

        for (auto & port_ctx : input_ports_with_data)
            port_ctx.input_port->close();

        return Status::Finished;
    }

    /// Do not disable inputs, so they can be executed in parallel.
    bool is_port_full = !output.canPush();

    /// Push if we have data.
    if (!output_chunks.empty() && !is_port_full)
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
    }

    Status status = Status::NeedData;

    for (size_t i = 0; auto & input_port_with_data : input_ports_with_data)
    {
        if (input_port_with_data.input_chunk || input_port_with_data.input_chunk.requestCheckpoint())
        {
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
                input_port_with_data.input_chunk = input_port_with_data.input_port->pull();
                status = Status::Ready;
            }
        }
        ++i;
    }

    if (is_port_full)
        return Status::PortFull;

    return status;
}

void JoinTransform::work()
{
    int64_t local_watermark = std::numeric_limits<int64_t>::max();

    bool has_watermark = false;
    bool has_data = false;
    bool all_requested_checkpoint = true;

    Chunks chunks;
    {
        /// Move out the input chunks
        std::scoped_lock lock(mutex);

        assert(input_ports_with_data[0].input_chunk || input_ports_with_data[1].input_chunk);

        for (size_t i = 0; i < input_ports_with_data.size(); ++i)
        {
            auto & input_chunk = input_ports_with_data[i].input_chunk;
            if (input_chunk)
            {
                if (input_chunk.hasWatermark())
                {
                    auto watermark = input_chunk.getChunkContext()->getWatermark();

                    local_watermark = std::min(local_watermark, watermark);
                    has_watermark = true;
                }
                else if (!input_chunk.requestCheckpoint())
                    all_requested_checkpoint = false;

                if (input_chunk.hasRows())
                    has_data = true;

                chunks[i].swap(input_chunk);
            }
        }

        /// We propagate empty chunk with or without watermark
        if (!has_data)
            output_chunks.emplace_back(output_header_chunk.clone());
    }

    if (has_data)
        doJoin(std::move(chunks));

    /// Piggy-back watermark
    /// We only do this piggy-back once for the last output chunk if there is
    if (has_watermark)
    {
        std::scoped_lock lock(mutex);
        if (!output_chunks.empty())
            setupWatermark(output_chunks.back(), local_watermark);
        else
            /// If there is no join result or chunks don't have data but have watermark, we still need propagate the watermark
            propagateWatermark(local_watermark);
    }
    else if (all_requested_checkpoint)
    {
        std::scoped_lock lock(mutex);
        checkpoint(chunks.front().getCheckpointContext());
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
        chunk.getOrCreateChunkContext()->setWatermark(local_watermark);
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
            {
                std::scoped_lock lock(mutex);
                output_chunks.emplace_back(joined_block.getColumns(), rows);
            }
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

        if (auto rows = block.rows(); rows > 0)
        {
            std::scoped_lock lock(mutex);

            /// First emit retracted block
            auto retracted_block_rows = retracted_block.rows();
            if (retracted_block_rows)
            {
                /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
                auto chunk_ctx = std::make_shared<ChunkContext>();
                chunk_ctx->setAvoidWatermark();
                output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
            }

            output_chunks.emplace_back(block.getColumns(), rows);
        }
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

        std::scoped_lock lock(mutex);

        for (size_t j = 0; j < joined_blocks.size(); ++j)
            output_chunks.emplace_back(joined_blocks[j].getColumns(), joined_blocks[j].rows());
    }
}

void JoinTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        /// TODO: join state
        // join->serialize(wb);
        DB::writeIntBinary(watermark, wb);
    });
}

void JoinTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
        /// TODO: join state
        // join->deserialize(rb);
        DB::readIntBinary(watermark, rb);
    });
}
}
}
