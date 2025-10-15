#include <Processors/Transforms/SquashingChunksTransform.h>
#include "Processors/ProcessorID.h"

/// proton: starts
#include <base/ClockUtils.h>
/// proton: ends

#include <iostream>


namespace DB
{

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory)
    : ExceptionKeepingTransform(header, header, false, ProcessorID::SquashingChunksTransformID)
    , squashing(min_block_size_rows, min_block_size_bytes, reserve_memory)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    /// proton: starts. Measure rows/bytes/time for squashing
    auto start_ns = MonotonicNanoseconds::now();
    auto in_rows = chunk.getNumRows();
    auto in_bytes = chunk.bytes();
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
    {
        cur_chunk.setColumns(block.getColumns(), block.rows());
    }
    metrics.processed_rows += in_rows;
    metrics.processed_bytes += in_bytes;
    metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
    /// proton: ends
}

SquashingChunksTransform::GenerateResult SquashingChunksTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void SquashingChunksTransform::onFinish()
{
    auto block = squashing.add({});
    finish_chunk.setColumns(block.getColumns(), block.rows());
}

void SquashingChunksTransform::work()
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

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ISimpleTransform(header, header, true, ProcessorID::SimpleSquashingChunksTransformID), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status SimpleSquashingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}

}
