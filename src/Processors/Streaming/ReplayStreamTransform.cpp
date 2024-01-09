#include <Processors/Streaming/ReplayStreamTransform.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/ProcessorID.h>
#include <base/ClockUtils.h>
#include <base/types.h>
#include <Common/ProtonCommon.h>

#include <thread>


namespace DB
{
namespace Streaming
{

constexpr Int64 MAX_WAIT_INTERVAL_MS = 500;

ReplayStreamTransform::ReplayStreamTransform(const Block & header, Float32 replay_speed_, Int64 last_sn_, const String & replay_time_col_)
    : ISimpleTransform(header, header, true, ProcessorID::ReplayStreamTransformID)
    , replay_speed(replay_speed_)
    , last_sn(last_sn_)
    , enable_replay(last_sn >= 0)
    , replay_time_col(replay_time_col_)
{
    time_index = header.getPositionByName(replay_time_col);
    auto & type = header.getByPosition(time_index).type;
    /// user defined replay_time_col must be DateTime64
    if (replay_time_col != ProtonConsts::RESERVED_APPEND_TIME && !isDateTime64(type))
            throw Exception(fmt::format("ReplayStreamTransform need datatime64 type column, but got {}", type->getName()), ErrorCodes::LOGICAL_ERROR);
    sn_index = header.getPositionByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
}

void ReplayStreamTransform::cutChunk(Chunk & input_chunk)
{
    assert(input_chunk.rows() > 0);
    size_t index = 0;
    size_t cur_index = 0;
    auto & columns = input_chunk.getColumns();

    auto cut_into_chunks = [&](size_t start_pos, size_t end_pos) {
        Chunk chunk;
        for (const auto & col : columns)
            chunk.addColumn(col->cut(start_pos, end_pos - start_pos));
        output_chunks.push(Chunk());
        output_chunks.back().swap(chunk);
    };

    while (index < input_chunk.rows())
    {
        if (columns[time_index]->getInt(index) != columns[time_index]->getInt(cur_index))
        {
            cut_into_chunks(cur_index, index);
            cur_index = index;
        }
        ++index;
    }

    cut_into_chunks(cur_index, index);
    input_chunk.clear();
}

void ReplayStreamTransform::work()
{
    if (input_data.exception)
    {
        /// Skip transform in case of exception.
        output_data = std::move(input_data);
        has_input = false;
        has_output = true;
        return;
    }

    /// proton: starts.
    auto start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += input_data.chunk.bytes();
    /// proton: ends.
    try
    {
        if (enable_replay && input_data.chunk.rows() && output_chunks.empty())
            cutChunk(input_data.chunk);
        transform(input_data.chunk, output_data.chunk);

        metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
        /// proton: ends.
    }
    catch (DB::Exception &)
    {
        output_data.exception = std::current_exception();
        has_output = true;
        has_input = false;
        return;
    }

    /// proton: starts. For case like SELECT count(*) FROM table WHERE expression EMIT STREAM PERIODIC INTERVAL 2 SECOND
    /// the output.header is empty. So we need explicitly check watermark chunk info here to propagate empty chunk
    /// with watermark.
    /// P.S. need propagate other chunk context like checkpoint context etc as well
    if (!skip_empty_chunks || output_data.chunk || output_data.chunk.hasChunkContext())
        has_output = true;
    /// proton: ends

    if (has_output && !output_data.chunk && getOutputPort().getHeader())
        /// Support invariant that chunks must have the same number of columns as header.
        output_data.chunk = Chunk(
            getOutputPort().getHeader().cloneEmpty().getColumns(),
            0,
            output_data.chunk.getChunkInfo(),
            output_data.chunk.getChunkContext()); /// proton : propagate chunk context
}

void ReplayStreamTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    if (!enable_replay || output_chunks.empty())
    {
        output_chunk.swap(input_chunk);
        has_input = false;
        return;
    }
    
    output_chunk.swap(output_chunks.front());
    // output_chunk.swap(output_chunks.front());
    output_chunks.pop();

    const auto & columns = output_chunk.getColumns();
    auto this_batch_last_sn = columns[sn_index]->getInt(output_chunk.rows() - 1);

    /// mark the historical data replay end and begin stream query.
    if (this_batch_last_sn >= last_sn && output_chunks.empty())
        enable_replay = false;

    auto this_batch_time = columns[time_index]->getInt(0);
    wait_interval_ms
        = static_cast<Int64>(std::lround((last_batch_time.has_value() ? this_batch_time - last_batch_time.value() : 0) / replay_speed));
    last_batch_time = this_batch_time;

    while (wait_interval_ms > 0)
    {
        if (isCancelled())
            return;

        auto sleep_interval_ms = wait_interval_ms > MAX_WAIT_INTERVAL_MS ? MAX_WAIT_INTERVAL_MS : wait_interval_ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_interval_ms));
        wait_interval_ms -= sleep_interval_ms;
    }
}
}
}
