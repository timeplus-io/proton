#include <Processors/ISink.h>

/// proton: starts.
#include <base/ClockUtils.h>
/// proton: ends.


namespace DB
{

ISink::ISink(Block header)
    : IProcessor({std::move(header)}, {}), input(inputs.front())
{
}

ISink::Status ISink::prepare()
{
    if (!was_on_start_called)
        return Status::Ready;

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void ISink::work()
{
    if (!was_on_start_called)
    {
        was_on_start_called = true;
        onStart();
    }
    else if (has_input)
    {
        /// proton: starts.
        auto start_ns = MonotonicNanoseconds::now();
        metrics.processed_bytes += current_chunk.bytes();
        /// proton: ends.

        has_input = false;
        consume(std::move(current_chunk));

        /// proton: starts.
        metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
        /// proton: ends.
    }
    else if (!was_on_finish_called)
    {
        was_on_finish_called = true;
        onFinish();
    }
}

}
