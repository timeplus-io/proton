#include <Processors/Streaming/ReplayStreamTransform.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ProcessorID.h>
#include <base/ClockUtils.h>
#include <base/types.h>
#include <Common/DateLUT.h>
#include <Common/IntervalKind.h>
#include <Common/logger_useful.h>
#include <Common/ProtonCommon.h>
#include <Common/assert_cast.h>

#include <thread>


namespace DB
{
namespace Streaming
{

constexpr Int64 MAX_WAIT_INTERVAL_US = 500000;
constexpr Int64 LOG_INTERVAL_US = 30000000;
constexpr Int64 MAX_WAIT_OUTPUT_INTERVAL_US = 60000000;

ReplayStreamTransform::ReplayStreamTransform(
    const Block & header, Float32 replay_speed_, Int64 last_sn_, const String & replay_time_col_, std::optional<String> start_time_, std::optional<String> end_time_)
    : IProcessor({std::move(header)}, {std::move(header)}, ProcessorID::ReplayStreamTransformID)
    , replay_time_col(replay_time_col_)
    , replay_speed(replay_speed_)
    , last_sn(last_sn_)
    , replay_finished(false)
    , logger(&Poco::Logger::get("ReplayStreamTransform"))
{
    time_index = header.getPositionByName(replay_time_col);
    /// user defined replay_time_col must be DateTime64
    sn_index = header.getPositionByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
    DataTypePtr replay_time_type = header.getDataTypes()[time_index];

    auto & timezone = (replay_time_col == ProtonConsts::RESERVED_APPEND_TIME || replay_time_col == ProtonConsts::RESERVED_INGEST_TIME
                       || replay_time_col == ProtonConsts::RESERVED_PROCESS_TIME)
        ? DateLUT::instance("UTC")
        : DateLUT::instance(getDateTimeTimezone(*replay_time_type));

    if (replay_time_col == ProtonConsts::RESERVED_APPEND_TIME || replay_time_col == ProtonConsts::RESERVED_INGEST_TIME
        || replay_time_col == ProtonConsts::RESERVED_PROCESS_TIME)
    {    
        time_scale = 3;
    }
    else
    {
        if (replay_time_type->getTypeId() == TypeIndex::DateTime64)
            time_scale = typeid_cast<const DataTypeDateTime64 *>(replay_time_type.get())->getScale();
    }

    if (time_scale > 3)
        LOG_WARNING(logger, "Using datetime64 with a scale greater than 3 may lead to inaccurate sequential display.");

    if (start_time_.value() != "")
    {
        ReadBufferFromString in(start_time_.value());
        DateTime64 res;
        parseDateTime64BestEffort(res, time_scale, in, timezone, DateLUT::instance("UTC"));
        last_batch_time = static_cast<Int64>(res);
    }

    if (end_time_.value() != "")
    {
        ReadBufferFromString in(end_time_.value());
        DateTime64 res;
        parseDateTime64BestEffort(res, time_scale, in, timezone, DateLUT::instance("UTC"));
        end_time = static_cast<Int64>(res);
    }

    if (last_batch_time.has_value() && end_time.has_value() && last_batch_time.value() > end_time.value())
    {
        reach_end_time = true;
        LOG_WARNING(logger, "The end time is earlier than the start time (start time: {}, end time: {}), resulting in no output.", last_batch_time.value(), end_time.value());
    }
}

ReplayStreamTransform::Status ReplayStreamTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// If reach end time.
    if (reach_end_time)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (output_chunk)
    {
        output.push(std::move(output_chunk));
        return Status::PortFull;
    }

    if (!chunks_to_replay.empty())
    {
        if (replay_finished)
        {
            output.push(std::move(chunks_to_replay.front()));
            chunks_to_replay.pop();
            return Status::PortFull;
        }
        return Status::Ready;
    }

    if (input_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(/*set_not_needed=*/true);

    /// After replay finshed, we just forward it
    if (replay_finished)
    {
        output.push(std::move(input_chunk));
        return Status::PortFull;
    }

    /// Now transform.
    return Status::Ready;
}

void ReplayStreamTransform::work()
{
    auto start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += input_chunk.bytes();
    if (input_chunk.hasRows())
    {
        chassert(chunks_to_replay.empty());
        chunks_to_replay = splitChunkByTime(); ///process input_chunk
    }

    if (!chunks_to_replay.empty())
        output_chunk = replayOneChunk();

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

/**
 * @brief put the row that has different time into different chunk.
 * For example: input_chunk: [1, 1, 1, 2, 2, 2, 3, 3, 3]
 * After the cutChunk, the chunks_to_replay will be [[1, 1, 1], [2, 2, 2], [3, 3, 3]]
 */

std::queue<Chunk> ReplayStreamTransform::splitChunkByTime()
{
    size_t chunk_rows = input_chunk.rows();
    chassert(chunk_rows > 0);

    const auto & columns = input_chunk.getColumns();
    const auto first_row_time = columns[time_index]->getInt(0);
    const auto last_row_time = columns[time_index]->getInt(input_chunk.rows() - 1);

    std::queue<Chunk> chunks;
    /// fast path, the whole chunk has the same time, no need to traverse one by one.
    if (last_row_time == first_row_time)
    {
        chunks.emplace(std::move(input_chunk));
        return chunks;
    }

    auto cut_into_chunks = [&](size_t start_row, size_t end_row) {
        Chunk chunk;
        for (const auto & col : columns)
            chunk.addColumn(col->cut(start_row, end_row - start_row));
        chunks.emplace(std::move(chunk));
    };

    size_t row = 0;
    size_t cur_row = 0;
    while (row < chunk_rows)
    {
        if (columns[time_index]->getInt(row) != columns[time_index]->getInt(cur_row))
        {
            cut_into_chunks(cur_row, row);
            cur_row = row;
        }
        ++row;
    }

    cut_into_chunks(cur_row, row);
    input_chunk.clear();
    return chunks;
}

Chunk ReplayStreamTransform::replayOneChunk()
{
    chassert(!chunks_to_replay.empty());

    /**
     * mark the historical data replay end and begin stream query.
     * If insert a block such as insert into test_rep(id, time_col) values (1, '2020-02-02 20:01:14')(2, '2020-02-02 20:01:14')(3, '2020-02-02 20:01:17')(4, '2020-02-02 20:01:20');
     * After the cutChunk, the chunks_to_replay will be [[1, '2020-02-02 20:01:14'], [2, '2020-02-02 20:01:14'], [3, '2020-02-02 20:01:17'], [4, '2020-02-02 20:01:20']], and the last_sn will be 0.
     * And each chunk in the chunks_to_replay will have the same time sn, when [1, '2020-02-02 20:01:14'] chunk came, this_batch_last_sn >= last_sn, but the chunks_to_replay is not empty,
     * we still need to replay the remain data.
    */
    auto now = MonotonicMicroseconds::now();
    if (!initialized)
    {
        initialized = true;

        replay_clock = now;

        if (!last_batch_time.has_value())
        {
            auto chunk = std::move(chunks_to_replay.front());
            chunks_to_replay.pop();
            last_batch_time = chunk.getColumns()[time_index]->getInt(0);
  
            if (end_time.has_value() && last_batch_time.value() >= end_time.value())
            {
                reach_end_time = true;
                return Chunk{};
            }

            return chunk; /// No sleep for first batch if there is no \replay_start_times
        }
    }

    Chunk chunk;
    Int64 wait_interval_us = 0;
    do
    {
        chunk = std::move(chunks_to_replay.front());
        chunks_to_replay.pop();

        auto this_batch_time = chunk.getColumns()[time_index]->getInt(0);
        wait_interval_us
            = static_cast<Int64>(std::lround((this_batch_time - last_batch_time.value()) * std::pow(10, 6 - time_scale) / replay_speed));
        if (wait_interval_us < 0)
        {
            if (now - last_unordered_log_ts > LOG_INTERVAL_US)
            {
                LOG_WARNING(
                    logger,
                    "Found unordered replay timestamp '{}', current_ts: {}, last_ts: {}",
                    replay_time_col,
                    this_batch_time,
                    last_batch_time.value());
                last_unordered_log_ts = now;
            }

            chunk.clear();
            continue;
        }

        last_batch_time = this_batch_time;
        break;
    } while (!chunks_to_replay.empty());

    if (end_time.has_value() && last_batch_time.value() >= end_time.value())
    {
        reach_end_time = true;
        return Chunk{};
    } 

    if (wait_interval_us > MAX_WAIT_OUTPUT_INTERVAL_US)
        LOG_WARNING(logger, "Next replaying data output may be slow, need to wait {}s", wait_interval_us / std::pow(10, 6));

    if (!chunk)
        return Chunk(getOutputPort().getHeader().getColumns(), 0); /// proton : propagate chunk context;

    const auto & columns = chunk.getColumns();

    if (chunks_to_replay.empty())
    {
        auto this_batch_last_sn = columns[sn_index]->getInt(chunk.rows() - 1);
        if (this_batch_last_sn >= last_sn)
            replay_finished = true;
    }

    if (now > replay_clock + wait_interval_us)
    {
        if (now - last_log_ts >= LOG_INTERVAL_US)
        {
            LOG_WARNING(
                logger,
                "Delay occured in replay stream, delayed {} microseconds, output may be inaccurate",
                now - (wait_interval_us + replay_clock));
            last_log_ts = now;
        }
    }

    while (wait_interval_us > 0)
    {
        if (isCancelled())
            return chunk;

        auto sleep_interval_us = std::min(MAX_WAIT_INTERVAL_US, wait_interval_us);
        replay_clock += sleep_interval_us;
        wait_interval_us -= sleep_interval_us;
        std::this_thread::sleep_until(std::chrono::time_point<std::chrono::steady_clock>(std::chrono::microseconds(replay_clock)));
    }
    return chunk;
}
}
}
