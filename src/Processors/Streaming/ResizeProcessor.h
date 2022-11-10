#pragma once

#include <Processors/IProcessor.h>
#include <queue>


namespace DB
{

namespace Streaming
{
/** Has arbitrary non zero number of inputs and arbitrary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data from arbitrary input (whenever it is ready) and pushes it to arbitrary output (whenever is is not full).
  * Doesn't do any heavy calculations.
  * Doesn't preserve an order of data.
  *
  * Examples:
  * - union data from multiple inputs to single output - to serialize data that was processed in parallel.
  * - split data from single input to multiple outputs - to allow further parallel processing.
  *
  * Resize processor needs rewritten for streaming processing since
  * 1. We may need align watermarks
  * 2. We may need propagate checkpoint barrier
  * Since resize processor can be arbitrary M inputs -> N outputs dimension mapping, this introduces some challenges when
  * dealing with watermarks and checkpoint barrier propagations
  *     Input -> Output
  * Case 1: M -> M:
  *   a. For watermarks, don't need alignment, (actually defer the watermark alignment to down stream pipe)
  *   b. For checkpoint barriers, don't need alignment
  * Case 2: M -> 1 (M > 1)
  *   a. For watermarks, need wait for all watermarks from all M inputs and pick the smallest watermark as the watermark
  *   b. For checkpoint barriers, need wait for all checkpoint barriers from all M inputs and then propagate the checkpoint
  *      barrier further to the down stream
  * Case 3: 1 -> N (N > 1)
  *   a. For watermarks, replicate watermark to all N outputs
  *   b. For checkpoint barriers, replicate checkpoint barriers to all N outputs
  * Case 4: M -> N (M != N, M > 1, N > 1)
  *   a. For watermarks, don't support this combination
  *   b. For checkpoint barriers, replicate checkpoint barriers to all N outputs
  */
class ResizeProcessor final : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    ResizeProcessor(const Block & header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header), ProcessorID::ResizeProcessorID)
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
    }

    String getName() const override { return "Resize"; }

    Status prepare() override;
    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> waiting_outputs;
    std::queue<UInt64> inputs_with_data;
    bool initialized = false;

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus
    {
        NotActive,
        HasData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
};

class StrictResizeProcessor final : public IProcessor
{
public:
    /// TODO Check that there is non zero number of inputs and outputs.
    StrictResizeProcessor(const Block & header, size_t num_inputs, size_t num_outputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header), ProcessorID::StrictResizeProcessorID)
        , current_input(inputs.begin())
        , current_output(outputs.begin())
    {
    }

    String getName() const override { return "StrictResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    InputPorts::iterator current_input;
    OutputPorts::iterator current_output;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> disabled_input_ports;
    std::queue<UInt64> waiting_outputs;
    bool initialized = false;

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
        ssize_t waiting_output;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    /// This field contained chunks which were read for output which had became finished while reading was happening.
    /// They will be pushed to any next waiting output.
    std::vector<Port::Data> abandoned_chunks;
};

}
}
