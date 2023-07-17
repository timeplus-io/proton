#include <Processors/ConcatProcessor.h>

/// proton: starts.
#include <Checkpoint/CheckpointContext.h>
/// proton ends.

namespace DB
{

ConcatProcessor::ConcatProcessor(const Block & header, size_t num_inputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header}, ProcessorID::ConcatProcessorID), current_input(inputs.begin())
{
}

ConcatProcessor::Status ConcatProcessor::prepare()
{
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        /// proton: starts.
        for (auto & input : inputs)
            input.close();
        /// proton: ends.

        return Status::Finished;
    }

    if (!output.isNeeded())
    {
        if (current_input != inputs.end())
            current_input->setNotNeeded();

        return Status::PortFull;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Check can input.

    while (current_input != inputs.end() && current_input->isFinished())
        ++current_input;

    if (current_input == inputs.end())
    {
        /// proton: starts.
        for (auto & input : inputs)
            input.close();
        /// proton: ends.

        output.finish();
        return Status::Finished;
    }

    auto & input = *current_input;

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    /// Move data.
    output.push(input.pull());

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

/// proton: starts.
void ConcatProcessor::recover(CheckpointContextPtr ckpt_ctx)
{
    if (isStreaming())
    {
        /// If has checkpoint, close all historical inputs, since we shall continue to consume from streaming source start with recovered `sn`
        /// Normally, streaming concat processor is used for VersionedKV/ChangelogKV:
        /// Like to read historical data first and then stream data

        // assert(ckpt_ctx->epoch > 0);
        if (ckpt_ctx->epoch > 0)
        {
            for (; current_input != inputs.end(); ++current_input)
            {
                if (current_input->getOutputPort().getProcessor().isStreaming())
                    break;

                /// Close these inputs in `prepare()`
                // current_input->close(); /// Close historical input
            }

            /// Always has at least one streaming input
            assert(current_input != inputs.end() && current_input->getOutputPort().getProcessor().isStreaming());
        }
    }
}
/// proton: ends.

}
