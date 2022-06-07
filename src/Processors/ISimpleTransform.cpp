#include <Processors/ISimpleTransform.h>

/// proton: starts
#include <base/ClockUtils.h>
/// proton: ends

namespace DB
{

ISimpleTransform::ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_)
    : IProcessor({std::move(input_header_)}, {std::move(output_header_)})
    , input(inputs.front())
    , output(outputs.front())
    , skip_empty_chunks(skip_empty_chunks_)
{
}

ISimpleTransform::Status ISimpleTransform::prepare()
{
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
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;

        if (!no_more_data_needed)
            return Status::PortFull;

    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void ISimpleTransform::work()
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
        transform(input_data.chunk, output_data.chunk);
        /// proton: starts.
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

    has_input = !needInputData();

    /// proton: starts. For case like SELECT count(*) FROM table WHERE expression EMIT STREAM PERIODIC INTERVAL 2 SECOND
    /// the output.header is empty. So we need explicitly check watermark chunk info here to propagate empty chunk
    /// with watermark
    if (!skip_empty_chunks || output_data.chunk || output_data.chunk.hasWatermark())
        has_output = true;
    /// proton: ends

    if (has_output && !output_data.chunk && getOutputPort().getHeader())
        /// Support invariant that chunks must have the same number of columns as header.
        output_data.chunk = Chunk(getOutputPort().getHeader().cloneEmpty().getColumns(), 0);
}

}

