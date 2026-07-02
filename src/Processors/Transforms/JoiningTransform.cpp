#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/JoinUtils.h>

/// proton: starts.
#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
#include <base/scope_guard.h>
/// proton: ends.

/// proton: starts.
#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
/// proton: ends.

/// proton: starts.
#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block JoiningTransform::transformHeader(Block header, const JoinPtr & join)
{
    LOG_DEBUG(getLogger("JoiningTransform"), "Before join block: '{}'", header.dumpStructure());
    /// proton: starts. Add for join hybrid hash table
    if (auto hash_join = std::dynamic_pointer_cast<Streaming::IHashJoin>(join))
    {
        hash_join->transformHeader(header);
    }
    else
    {
        join->checkTypesOfKeys(header);
        join->initialize(header);
        ExtraBlockPtr tmp;
        join->joinBlock(header, tmp);
    }
    /// proton: ends.
    LOG_DEBUG(getLogger("JoiningTransform"), "After join block: '{}'", header.dumpStructure());
    return header;
}

JoiningTransform::JoiningTransform(
    const Block & input_header,
    const Block & output_header,
    JoinPtr join_,
    size_t max_block_size_,
    bool on_totals_,
    bool default_totals_,
    FinishCounterPtr finish_counter_)
    : IProcessor({input_header}, {output_header}, ProcessorID::JoiningTransformID)
    , join(std::move(join_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
    , finish_counter(std::move(finish_counter_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        inputs.emplace_back(Block(), this); // Wait for FillingRightJoinSideTransform

    /// proton: starts. Add for join hybrid hash table
    if (auto hash_join = std::dynamic_pointer_cast<Streaming::IHashJoin>(join))
        hash_join->postInit(input_header, output_header, /*join_max_cached_bytes=*/std::numeric_limits<size_t>::max());
    /// proton: ends.
}

JoiningTransform::~JoiningTransform() = default;

OutputPort & JoiningTransform::getFinishedSignal()
{
    assert(outputs.size() == 2);
    return outputs.back();
}

IProcessor::Status JoiningTransform::prepare()
{
    auto & output = outputs.front();
    auto & on_finish_output = outputs.back();

    /// Check can output.
    if (output.isFinished() || stop_reading)
    {
        output.finish();
        on_finish_output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;

        return Status::PortFull;
    }

    if (inputs.size() > 1)
    {
        auto & last_in = inputs.back();
        if (!last_in.isFinished())
        {
            last_in.setNeeded();
            if (last_in.hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected from second JoiningTransform port");

            return Status::NeedData;
        }
    }

    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        if (process_non_joined)
            return Status::Ready;

        output.finish();
        on_finish_output.finish();
        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void JoiningTransform::work()
{
    if (has_input)
    {
        /// proton: starts.
        auto start_ns = MonotonicNanoseconds::now();
        metrics.processed_bytes += input_chunk.bytes();
        metrics.processed_rows += input_chunk.rows();
        SCOPE_EXIT({ metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns; });
        /// proton: ends.

        transform(input_chunk);
        output_chunk.swap(input_chunk);
        has_input = not_processed != nullptr;
        has_output = !output_chunk.empty();
    }
    else
    {
        if (!non_joined_blocks)
        {
            if (!finish_counter || !finish_counter->isLast())
            {
                process_non_joined = false;
                return;
            }

            non_joined_blocks = join->getNonJoinedBlocks(
                inputs.front().getHeader(), outputs.front().getHeader(), max_block_size);
            if (!non_joined_blocks)
            {
                process_non_joined = false;
                return;
            }
        }

        Block block = non_joined_blocks->next();
        if (!block)
        {
            process_non_joined = false;
            return;
        }

        auto rows = block.rows();
        output_chunk.setColumns(block.getColumns(), rows);
        has_output = true;
    }
}

void JoiningTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        /// proton: starts.
        LOG_INFO(
            getLogger("JoiningTransform"),
            "Built right-side join state ({}): rows={}, bytes={}",
            join->getName(),
            join->getTotalRowCount(),
            join->getTotalByteCount());
        /// proton: ends.

        if (join->alwaysReturnsEmptySet() && !on_totals)
        {
            stop_reading = true;
            chunk.clear();
            return;
        }
    }

    Block block;
    if (on_totals)
    {
        const auto & left_totals = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        const auto & right_totals = join->getTotals();

        /// Drop totals if both out stream and joined stream doesn't have ones.
        /// See comment in ExpressionTransform.h
        if (default_totals && !right_totals)
            return;

        block = outputs.front().getHeader().cloneEmpty();
        JoinCommon::joinTotals(left_totals, right_totals, join->getTableJoin(), block);
    }
    else
        block = readExecute(chunk);
    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

Block JoiningTransform::readExecute(Chunk & chunk)
{
    Block res;

    if (!not_processed)
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        if (res)
            join->joinBlock(res, not_processed);
    }
    else if (not_processed->empty()) /// There's not processed data inside expression.
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        not_processed.reset();
        join->joinBlock(res, not_processed);
    }
    else
    {
        res = std::move(not_processed->block);
        join->joinBlock(res, not_processed);
    }

    return res;
}

FillingRightJoinSideTransform::FillingRightJoinSideTransform(Block input_header, JoinPtr join_)
    : IProcessor({input_header}, {Block()}, ProcessorID::FillingRightJoinSideTransformID)
    , join(std::move(join_))
{}

/// proton: starts.
String FillingRightJoinSideTransform::getName() const
{
    if (auto hash_join = std::dynamic_pointer_cast<Streaming::IHashJoin>(join))
        return fmt::format("FillingRightJoinSide({})", hash_join->type());
    else
        return "FillingRightJoinSide";
}
/// proton: ends.

InputPort * FillingRightJoinSideTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals port was already added to FillingRightJoinSideTransform");

    return &inputs.emplace_back(inputs.front().getHeader(), this);
}

IProcessor::Status FillingRightJoinSideTransform::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    auto & input = inputs.front();

    if (stop_reading)
    {
        input.close();
    }
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }
    else if (!set_totals)
    {
        chunk.setColumns(inputs.front().getHeader().cloneEmpty().getColumns(), 0);
        for_totals = true;
        return Status::Ready;
    }

    output.finish();
    return Status::Finished;
}

void FillingRightJoinSideTransform::work()
{
    /// proton: starts. Measure rows/bytes/time for building right side
    auto start_ns = MonotonicNanoseconds::now();
    auto rows = chunk.getNumRows();
    auto bytes = chunk.bytes();
    auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

    if (for_totals)
        join->setTotals(block);
    else
        stop_reading = !join->addJoinedBlock(block);

    set_totals = for_totals;
    metrics.processed_rows += rows;
    metrics.processed_bytes += bytes;
    metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
    /// proton: ends
}

DelayedJoinedBlocksWorkerTransform::DelayedJoinedBlocksWorkerTransform(
    Block left_header_,
    Block output_header_,
    size_t max_block_size_,
    JoinPtr join_)
    : IProcessor(InputPorts{Block()}, OutputPorts{output_header_}, ProcessorID::DelayedJoinedBlocksWorkerTransformID)
    , left_header(left_header_)
    , output_header(output_header_)
    , max_block_size(max_block_size_)
    , join(join_)
{
}

IProcessor::Status DelayedJoinedBlocksWorkerTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

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

    if (inputs.size() != 1 && outputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform must have exactly one input port");

    if (output_chunk)
    {
        input.setNotNeeded();

        if (!output.canPush())
            return Status::PortFull;

        output.push(std::move(output_chunk));
        output_chunk.clear();
        return Status::PortFull;
    }

    if (!task)
    {
        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        auto data = input.pullData(true);
        if (data.exception)
        {
            output.pushException(data.exception);
            return Status::Finished;
        }

        if (!data.chunk.hasChunkInfo())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform must have chunk info");
        task = std::dynamic_pointer_cast<const DelayedBlocksTask>(data.chunk.getChunkInfo());

    }
    else
    {
        input.setNotNeeded();
    }

    if (task->finished)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    return Status::Ready;
}

void DelayedJoinedBlocksWorkerTransform::work()
{
    if (!task)
        return;

    Block block;
    if (!left_delayed_stream_finished)
    {
        block = task->delayed_blocks->next();

        if (!block)
        {
            left_delayed_stream_finished = true;
            block = nextNonJoinedBlock();
        }
    }
    else
    {
        block = nextNonJoinedBlock();
    }
    if (!block)
    {
        resetTask();
        return;
    }

    // Add block to the output
    auto rows = block.rows();
    output_chunk.setColumns(block.getColumns(), rows);
}

void DelayedJoinedBlocksWorkerTransform::resetTask()
{
    task.reset();
    left_delayed_stream_finished = false;
    setup_non_joined_stream = false;
    non_joined_delayed_stream = nullptr;
}

Block DelayedJoinedBlocksWorkerTransform::nextNonJoinedBlock()
{
    if (!setup_non_joined_stream)
    {
        setup_non_joined_stream = true;
        // Before read from non-joined stream, all blocks in left file reader must have been joined.
        // For example, in HashJoin, it may return invalid mismatch rows from non-joined stream before
        // the all blocks in left file reader have been finished, since the used flags are incomplete.
        // To make only one processor could read from non-joined stream seems be a easy way.
        if (task && task->left_delayed_stream_finish_counter->isLast())
        {
            if (!non_joined_delayed_stream)
            {
                non_joined_delayed_stream = join->getNonJoinedBlocks(left_header, output_header, max_block_size);
            }
        }
    }
    if (non_joined_delayed_stream)
    {
        return non_joined_delayed_stream->next();
    }
    return {};
}

DelayedJoinedBlocksTransform::DelayedJoinedBlocksTransform(size_t num_streams, JoinPtr join_)
    : IProcessor(InputPorts{}, OutputPorts(num_streams, Block()), ProcessorID::DelayedJoinedBlocksTransformID)
    , join(std::move(join_))
{
}

void DelayedJoinedBlocksTransform::work()
{
    if (finished)
        return;

    delayed_blocks = join->getDelayedBlocks();
    finished = finished || delayed_blocks == nullptr;
}

IProcessor::Status DelayedJoinedBlocksTransform::prepare()
{
    for (auto & output : outputs)
    {
        if (output.isFinished())
        {
            /// If at least one output is finished, then we have read all data from buckets.
            /// Some workers can still be busy with joining the last chunk of data in memory,
            /// but after that they also will finish when they will try to get next chunk.
            finished = true;
            continue;
        }
        if (!output.canPush())
            return Status::PortFull;
    }

    if (finished)
    {
        // Since have memory limit, cannot handle all buckets parallelly by different
        // DelayedJoinedBlocksWorkerTransform. So send the same task to all outputs.
        // Wait for all DelayedJoinedBlocksWorkerTransform be idle before getting next bucket.
        for (auto & output : outputs)
        {
            if (output.isFinished())
                continue;
            Chunk chunk;
            chunk.setChunkInfo(std::make_shared<DelayedBlocksTask>());
            output.push(std::move(chunk));
            output.finish();
        }

        return Status::Finished;
    }

    if (delayed_blocks)
    {
        // This counter is used to ensure that only the last DelayedJoinedBlocksWorkerTransform
        // could read right non-joined blocks from the join.
        auto left_delayed_stream_finished_counter = std::make_shared<JoiningTransform::FinishCounter>(outputs.size());
        for (auto & output : outputs)
        {
            Chunk chunk;
            auto task = std::make_shared<DelayedBlocksTask>(delayed_blocks, left_delayed_stream_finished_counter);
            chunk.setChunkInfo(task);
            output.push(std::move(chunk));
        }
        delayed_blocks = nullptr;
        return Status::PortFull;
    }

    return Status::Ready;
}

}
