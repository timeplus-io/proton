#include <Processors/ISource.h>

/// proton: starts
#include <base/ClockUtils.h>
#include <Common/ProfileEvents.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::ISource(Block header, ProcessorID pid_)
    : IProcessor({}, {std::move(header)}, pid_), output(outputs.front())
{
}

ISource::Status ISource::prepare()
{
    if (finished || isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::work()
{
    try
    {
        /// proton: starts.
        auto start_ns = MonotonicNanoseconds::now();
        /// proton: ends.
        if (auto chunk = tryGenerate())
        {
            /// proton: starts.
            metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
            metrics.processed_bytes += chunk->bytes();
            /// proton: ends.
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
                has_input = true;
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}

}

