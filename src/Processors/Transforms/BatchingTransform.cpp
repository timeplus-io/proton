#include <Processors/Transforms/BatchingTransform.h>

#include <Common/logger_useful.h>
/// proton: starts
#include <base/ClockUtils.h>
/// proton: ends

namespace DB
{

BatchingTransform::BatchingTransform(const Block & header, size_t batch_size_rows, UInt64 timeout_ms_, bool reserve_memory)
    : ExceptionKeepingTransform(header, header, false, ProcessorID::BatchingTransformID)
    , squashing(batch_size_rows, std::numeric_limits<size_t>::max(), reserve_memory)
    , timeout_ms(timeout_ms_)
{
}

void BatchingTransform::onConsume(Chunk chunk)
{
    /// proton: starts. Measure rows/bytes/time for batching
    auto start_ns = MonotonicNanoseconds::now();
    auto in_rows = chunk.getNumRows();
    auto in_bytes = chunk.bytes();
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
    {
        cur_chunk.setColumns(block.getColumns(), block.rows());
        timer.restart();
    }
    else if (timer.elapsedMilliseconds() >= timeout_ms)
    {
        if (auto blk = squashing.add({}))
            cur_chunk.setColumns(blk.getColumns(), blk.rows());
        timer.restart();
    }
    metrics.processed_rows += in_rows;
    metrics.processed_bytes += in_bytes;
    metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
    /// proton: ends
}

BatchingTransform::GenerateResult BatchingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void BatchingTransform::onFinish()
{
    auto block = squashing.add({});
    finish_chunk.setColumns(block.getColumns(), block.rows());
}

void BatchingTransform::work()
{
    if (stage == Stage::Exception)
    {
        data.chunk.clear();
        ready_input = false;
        return;
    }

    ExceptionKeepingTransform::work();
    if (finish_chunk)
    {
        data.chunk = std::move(finish_chunk);
        ready_output = true;
    }
}

}
