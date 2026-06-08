#include <Processors/Transforms/Streaming/JoinTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>
#include <Interpreters/TableJoin.h>
#include <Processors/Transforms/Streaming/JoinRightBoundary.h>
#include <base/ClockUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
namespace
{
bool isHistoricalBoundaryMarker(const Chunk & chunk)
{
    auto chunk_ctx = chunk.getChunkContext();
    return !chunk.hasRows() && chunk_ctx && (chunk_ctx->isHistoricalDataStart() || chunk_ctx->isHistoricalDataEnd());
}

}

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
    size_t transform_id_,
    size_t max_block_size_,
    UInt64 join_max_cached_bytes,
    JoinRightBoundaryPtr right_boundary_,
    JoinLeftBoundaryPtr left_boundary_)
    : IProcessor({left_input_header, right_input_header}, {output_header}, ProcessorID::StreamingJoinTransformID)
    , join(std::move(join_))
    , transform_id(transform_id_)
    , max_block_size(max_block_size_)
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , logger(getLogger("StreamingJoinTransform"))
    , input_ports_with_data{InputPortWithData{&inputs.front()}, InputPortWithData{&inputs.back()}}
    , right_boundary(std::move(right_boundary_))
    , left_boundary(std::move(left_boundary_))
    , right_boundary_ready(!right_boundary || right_boundary->isReleased())
    , right_boundary_local_reached(!right_boundary || right_boundary->isReleased())
    , left_boundary_released(!left_boundary)
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

    if (isCancelled())
    {
        abandonBoundaryParticipation("cancellation");
        for (auto & port_ctx : input_ports_with_data)
            port_ctx.input_port->close();

        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
    {
        abandonBoundaryParticipation("output finish");
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

    if (right_boundary && right_boundary_local_reached && !right_boundary_ready
        && (right_boundary->tryRelease() || right_boundary->isReleased()))
        return Status::Ready;

    if (left_boundary && !left_boundary_released && left_boundary->isReleased())
        return Status::Ready;

    auto can_pull_input = [&](size_t input_index) {
        if (required_update_processing_index.has_value() && *required_update_processing_index == input_index)
            return true;

        /// Keep boundary queues bounded. Once one chunk is delayed, withhold
        /// demand from that side until the matching boundary releases.
        if (input_index == 0 && right_boundary && !right_boundary_ready && !delayed_left_chunks.empty())
            return false;

        if (input_index == 1 && left_boundary && right_boundary_ready && !left_boundary_released && !delayed_right_chunks.empty())
            return false;

        return true;
    };

    Status status = Status::NeedData;

    for (size_t i = 0; i < input_ports_with_data.size(); ++i)
    {
        auto & input_port_with_data = input_ports_with_data[i];
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
            if (!can_pull_input(i))
                continue;

            input_port_with_data.input_port->setNeeded();

            if (input_port_with_data.input_port->hasData())
            {
                input_port_with_data.input_chunk = input_port_with_data.input_port->pull(true);
                status = Status::Ready;
            }
        }
    }

    return status;
}

void JoinTransform::work()
{
    chassert(right_boundary || left_boundary || input_ports_with_data[0].input_chunk || input_ports_with_data[1].input_chunk);

    auto start_ns = MonotonicNanoseconds::now();
    uint64_t in_rows = 0;
    uint64_t in_bytes = 0;
    int64_t local_watermark = std::numeric_limits<int64_t>::max();

    bool has_watermark = false;
    bool has_data = false;
    bool right_boundary_became_ready = false;
    UInt8 requested_checkpoint_num = 0;
    CheckpointContextPtr requested_ckpt;

    Chunks chunks;
    {
        /// Move out the input chunks
        if (right_boundary && right_boundary_local_reached && !right_boundary_ready
            && (right_boundary->tryRelease() || right_boundary->isReleased()))
            right_boundary_became_ready = markRightBoundaryReady("shared stop-SN boundary");

        if (left_boundary && !left_boundary_released && left_boundary->isReleased())
            observeLeftBoundaryReleased("shared historical left boundary");

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
                    chassert(!required_update_processing_index && "Watermark should not be present when update processing is required");

                    auto input_chunk_watermark = input_chunk.getChunkContext()->getWatermark();

                    local_watermark = std::min(local_watermark, input_chunk_watermark);
                    has_watermark = true;
                }
                else if (input_chunk.requestCheckpoint())
                {
                    chassert(!required_update_processing_index && "Checkpoint request should not occur when update processing is required");

                    if ((right_boundary || left_boundary) && hasCheckpointUnsafeBoundaryState())
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Checkpoint barrier reached streaming join key-domain boundary state before delayed chunks were replayed");
                    }

                    ++requested_checkpoint_num;
                    continue; /// keep in input_ports_with_data until all inputs checkpoint requested
                }

                if (i == 1 && right_boundary && input_chunk.getChunkContext() && input_chunk.getChunkContext()->isHistoricalDataEnd())
                {
                    right_boundary_local_reached = true;
                    if (right_boundary->markParticipantReached(transform_id))
                        right_boundary_became_ready = markRightBoundaryReady("historical-end marker") || right_boundary_became_ready;
                }

                if (input_chunk.hasRows())
                {
                    has_data = true;
                    in_rows += input_chunk.getNumRows();
                    in_bytes += input_chunk.bytes();
                }

                chunks[i].swap(input_chunk);
            }
        }

        /// All inputs request checkpoint
        if (requested_checkpoint_num == input_ports_with_data.size())
        {
            requested_ckpt = input_ports_with_data.front().input_chunk.getCheckpointContext();
            std::ranges::for_each(input_ports_with_data, [](auto & data) { data.input_chunk.clear(); });
        }
    }

    if (right_boundary && !right_boundary_ready && chunks[0] && (chunks[0].hasRows() || isHistoricalBoundaryMarker(chunks[0])))
    {
        LOG_TRACE(
            logger,
            "Delaying left chunk until right snapshot boundary: rows={}, historical_start={}, historical_end={}",
            chunks[0].getNumRows(),
            chunks[0].getChunkContext() && chunks[0].getChunkContext()->isHistoricalDataStart(),
            chunks[0].getChunkContext() && chunks[0].getChunkContext()->isHistoricalDataEnd());
        has_data = has_data && chunks[1].hasRows();
        delayed_left_chunks.emplace_back(std::move(chunks[0]));
    }
    else if (right_boundary && chunks[0] && isHistoricalBoundaryMarker(chunks[0]))
    {
        processLeftChunk(std::move(chunks[0]));
    }

    if (right_boundary && (!right_boundary_ready || !delayed_left_chunks.empty()))
        has_watermark = false;

    if (right_boundary && right_boundary_ready && left_boundary && !left_boundary->isReleased() && chunks[1].hasRows())
    {
        LOG_TRACE(
            logger,
            "Delaying right live chunk until left snapshot boundary: rows={}, left_progress={}",
            chunks[1].getNumRows(),
            left_boundary->progressString());
        has_data = has_data && chunks[0].hasRows();
        delayed_right_chunks.emplace_back(std::move(chunks[1]));
    }

    if (left_boundary && !left_boundary_released)
        has_watermark = false;

    if (has_data)
        doJoin(std::move(chunks));

    if (right_boundary_ready && (!delayed_left_chunks.empty() || right_boundary_became_ready))
        replayDelayedLeftChunks();

    if (left_boundary && !left_boundary_released && left_boundary->isReleased())
        observeLeftBoundaryReleased("shared historical left boundary");

    /// If no output was produced, emit a heartbeat chunk.
    /// Skip the heartbeat when the next "consecutive" chunk must be processed,
    /// to avoid downstream aggregation to emit transitive results we don't want
    if (output_chunks.empty() && !required_update_processing_index)
        output_chunks.emplace_back(output_header_chunk.clone());

    /// Piggy-back watermark
    /// We only do this piggy-back once for the last output chunk if there is
    if (has_watermark)
    {
        chassert(!output_chunks.empty());
        if (isHistoricalBoundaryMarker(output_chunks.back()))
            output_chunks.emplace_back(output_header_chunk.clone());
        setupWatermark(output_chunks.back(), local_watermark);
    }
    else if (requested_ckpt)
    {
        checkpoint(requested_ckpt);

        /// Propagate request checkpoint
        chassert(!output_chunks.empty());
        output_chunks.back().setCheckpointContext(std::move(requested_ckpt));
    }

    if (auto now = MonotonicSeconds::now(); now - last_log_ts > 60)
    {
        LOG_INFO(logger, "{}, watermark={}", join->metricsString(), watermark);
        last_log_ts = now;
    }

    metrics.processed_rows += in_rows;
    metrics.processed_bytes += in_bytes;
    metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
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

bool JoinTransform::markRightBoundaryReady(const char * reason)
{
    if (!right_boundary || right_boundary_ready)
        return false;

    right_boundary_ready = true;
    LOG_DEBUG(logger, "Streaming join right snapshot boundary reached by {}; source_progress={}", reason, right_boundary->progressString());
    return true;
}

bool JoinTransform::observeLeftBoundaryReleased(const char * reason)
{
    if (!left_boundary || left_boundary_released || !left_boundary->isReleased())
        return false;

    left_boundary_released = true;
    LOG_DEBUG(
        logger,
        "Streaming join left snapshot boundary released by {}; transform_id={}, left_progress={}, delayed_right_chunks={}",
        reason,
        transform_id,
        left_boundary->progressString(),
        delayed_right_chunks.size());
    processDelayedRightChunks();
    return true;
}

void JoinTransform::replayDelayedLeftChunks()
{
    while (!delayed_left_chunks.empty())
    {
        auto delayed_chunk = std::move(delayed_left_chunks.front());
        delayed_left_chunks.pop_front();
        LOG_TRACE(
            logger,
            "Replaying delayed left chunk after right snapshot boundary: rows={}, historical_start={}, historical_end={}",
            delayed_chunk.getNumRows(),
            delayed_chunk.getChunkContext() && delayed_chunk.getChunkContext()->isHistoricalDataStart(),
            delayed_chunk.getChunkContext() && delayed_chunk.getChunkContext()->isHistoricalDataEnd());
        processLeftChunk(std::move(delayed_chunk));
    }
}

void JoinTransform::processLeftChunk(Chunk chunk)
{
    if (chunk.hasRows())
    {
        Chunks chunks;
        chunks[0].swap(chunk);
        doJoin(std::move(chunks));
    }
    else if (auto chunk_ctx = chunk.getChunkContext();
             chunk_ctx && (chunk_ctx->isHistoricalDataStart() || chunk_ctx->isHistoricalDataEnd()))
    {
        auto marker = output_header_chunk.clone();
        marker.setChunkContext(std::move(chunk_ctx));
        marker.clearWatermark();
        output_chunks.emplace_back(std::move(marker));

        if (right_boundary && output_chunks.back().getChunkContext()->isHistoricalDataEnd())
        {
            if (!left_boundary)
            {
                LOG_DEBUG(
                    logger,
                    "Streaming join left snapshot boundary reached; releasing delayed right chunks={}",
                    delayed_right_chunks.size());
                left_boundary_released = true;
                processDelayedRightChunks();
            }
            else if (left_boundary->markParticipantEnded(transform_id))
            {
                observeLeftBoundaryReleased("historical-end marker");
            }
            else
            {
                LOG_DEBUG(
                    logger,
                    "Streaming join left snapshot boundary reached for transform_id={}; waiting for peers left_progress={}, "
                    "delayed_right_chunks={}",
                    transform_id,
                    left_boundary->progressString(),
                    delayed_right_chunks.size());
            }
        }
    }
}

void JoinTransform::processDelayedRightChunks()
{
    while (!delayed_right_chunks.empty())
    {
        auto delayed_chunk = std::move(delayed_right_chunks.front());
        delayed_right_chunks.pop_front();

        if (!delayed_chunk.hasRows())
            continue;

        LOG_TRACE(logger, "Replaying delayed right chunk after left snapshot boundary: rows={}", delayed_chunk.getNumRows());
        Chunks chunks;
        chunks[1].swap(delayed_chunk);
        doJoin(std::move(chunks));
    }
}

bool JoinTransform::hasCheckpointUnsafeBoundaryState() const
{
    return !right_boundary_ready || !left_boundary_released || !delayed_left_chunks.empty() || !delayed_right_chunks.empty();
}

void JoinTransform::abandonBoundaryParticipation(const char * reason) noexcept
{
    try
    {
        if (right_boundary && !right_boundary_local_reached)
        {
            right_boundary_local_reached = true;
            if (right_boundary->markParticipantReached(transform_id) || right_boundary->isReleased())
                markRightBoundaryReady(reason);
        }

        if (left_boundary && !left_boundary_released)
        {
            if (left_boundary->markParticipantEnded(transform_id) || left_boundary->isReleased())
            {
                left_boundary_released = true;
                LOG_DEBUG(
                    logger,
                    "Streaming join left snapshot boundary abandoned by {}; transform_id={}, left_progress={}, delayed_right_chunks={}",
                    reason,
                    transform_id,
                    left_boundary->progressString(),
                    delayed_right_chunks.size());
            }
        }

        delayed_left_chunks.clear();
        delayed_right_chunks.clear();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error while abandoning streaming join boundary participation.");
    }
}

void JoinTransform::markBoundaryParticipationAbandoned(const char * reason) noexcept
{
    try
    {
        if (right_boundary)
            right_boundary->markParticipantReached(transform_id);

        if (left_boundary)
            left_boundary->markParticipantEnded(transform_id);

        LOG_DEBUG(logger, "Streaming join boundary participation marked abandoned by {}; transform_id={}", reason, transform_id);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error while marking streaming join boundary participation abandoned.");
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
            /// Don't watermark retracted chunk since we like the retracted chunk and the following result chunk
            /// to process in a consecutive way. For example, avoid emitting result right after processing retracted chunk
            /// but without processing the following result chunk. This will prevent transitive emit result we don't like
            /// to have usually.
            /// To have retracted chunk / result chunk processed consecutively, we can either concat them into one bigger
            /// chunk or use `consecutive` flag which we use here.
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
            auto rows = joined_block.rows();
            if (rows)
                output_chunks.emplace_back(joined_block.detachColumns(), rows);
        }
    }
}

void JoinTransform::onCancel() noexcept
{
    try
    {
        markBoundaryParticipationAbandoned("cancellation");
        join->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error occurs on cancellation.");
    }
}

String JoinTransform::getName() const
{
    switch (join->type())
    {
        case HashJoinType::Memory:
            return "StreamingJoinTransform";
        case HashJoinType::Hybrid:
            return "HybridStreamingJoinTransform";
    }
}

}
}
