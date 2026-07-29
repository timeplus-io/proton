#include <Processors/Streaming/ResizeProcessor.h>

#include <base/ClockUtils.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
ShrinkResizeProcessor::ShrinkResizeProcessor(const Block & header, size_t num_inputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(1, header), ProcessorID::StreamingShrinkResizeProcessorID)
    , logger(getLogger("ShrinkResizeProcessor"))
{
    assert(num_inputs > 0);
    ckpt_aligning_stopwatch.reset();
}

IProcessor::Status ShrinkResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & /*updated_outputs*/)
{
    if (unlikely(!initialized))
    {
        initialized = true;

        input_ports.reserve(inputs.size());
        for (auto & input : inputs)
        {
            assert(input.getOutputPort().getProcessor().isStreaming());
            input.setNeeded();
            input_ports.push_back({.port = &input, .status = InputStatus::NeedData, .watermark = INVALID_WATERMARK});
        }
    }

    /// Update inputs
    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::HasData)
            {
                input.status = InputStatus::HasData;
                /// The pinned input is pulled directly via the exclusive path (by status, not the
                /// queue). Queueing it too would leave an entry the exclusive pull never pops —
                /// an unbounded leak under a hot input's steady (-1, +1) traffic.
                if (exclusive_input != input_number)
                    inputs_with_data.push(input_number);
            }
        }
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Check output can push
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Check inputs has data
    for (auto & input_port : input_ports)
    {
        if (input_port.status == InputStatus::NeedData)
            input_port.port->setNeeded();
    }

    checkAndLogSlowCheckpointAligning();

    /// Pick the input to service. While pinned mid-pair, take the pin's +1 partner before
    /// any other input. The pinned input is setNeeded above and kept out of inputs_with_data,
    /// so returning NeedData here cannot stall (the executor re-runs us once the +1 arrives)
    /// and every queued entry is a live HasData input (no staleness filter needed below).
    std::optional<UInt64> pull_input;
    if (exclusive_input.has_value())
    {
        const auto ex = *exclusive_input;
        if (input_ports[ex].status == InputStatus::HasData)
            pull_input = ex; /// the +1 partner arrived
        else if (input_ports[ex].status != InputStatus::Finished)
            return Status::NeedData; /// wait for the +1 partner; do not service other inputs
        else
            exclusive_input.reset(); /// pinned input finished without a +1 — release and fall through
    }

    if (!pull_input.has_value() && !inputs_with_data.empty())
    {
        pull_input = inputs_with_data.front();
        inputs_with_data.pop();
        chassert(input_ports[*pull_input].status == InputStatus::HasData);
    }

    if (pull_input.has_value())
    {
        auto & input_with_data = input_ports[*pull_input];

        auto start_ns = MonotonicNanoseconds::now();
        auto data = input_with_data.port->pullData(/*set_not_needed=*/true);

        /// A consecutive (-1) chunk opens a pair; pin this input until its partner is pulled.
        if (data.chunk.isConsecutiveData())
            exclusive_input = *pull_input;
        else
            exclusive_input.reset();

        if (updateAndAlignWatermark(input_with_data, data.chunk) || updateAndRequestCheckpoint(input_with_data, data.chunk))
        {
            /// Do nothing
        }
        else
        {
            /// Historical start/end mark from multiple inputs may conflict and will no longer take effect.
            data.chunk.clearHistoricalDataStartAndEnd();
            input_with_data.status = InputStatus::NeedData;
        }

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
            ++num_finished_inputs;
            if (exclusive_input == pull_input)
                exclusive_input.reset();
        }

        /// metrics
        metrics.processed_bytes += data.chunk.bytes();
        metrics.processed_rows += data.chunk.rows();

        output.pushData(std::move(data));
        metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
        return Status::PortFull;
    }

    return Status::NeedData;
}

bool ShrinkResizeProcessor::updateAndAlignWatermark(InputPortWithStatus & input_with_data, Chunk & chunk)
{
    if (!chunk.hasWatermark())
        return false;

    assert(!chunk.requestCheckpoint());

    bool updated = false;
    auto new_watermark = chunk.getWatermark();
    if (new_watermark > input_with_data.watermark || (input_with_data.watermark == TIMEOUT_WATERMARK && new_watermark >= aligned_watermark))
    {
        input_with_data.watermark = new_watermark;
        auto min_watermark
            = std::ranges::min(input_ports, [](const auto & l, const auto & r) { return l.watermark < r.watermark; }).watermark;
        if (min_watermark > aligned_watermark)
        {
            aligned_watermark = min_watermark;
            updated = true;
        }
    }
    else
    {
        if (unlikely(new_watermark < aligned_watermark))
            LOG_INFO(logger, "Found outdated watermark. aligned watermark={}, but got watermark = {}", aligned_watermark, new_watermark);
    }

    input_with_data.status = InputStatus::NeedData;

    if (updated)
        chunk.setWatermark(aligned_watermark);
    else
        chunk.clearWatermark();

    return true;
}

bool ShrinkResizeProcessor::updateAndRequestCheckpoint(InputPortWithStatus & input_with_data, Chunk & chunk)
{
    if (!chunk.requestCheckpoint())
        return false;

    if (!input_with_data.requested_checkpoint)
    {
        input_with_data.requested_checkpoint = true;
        /// Start the stopwatch when the first checkpoint request came in
        if (num_requested_checkpoint++ == 0)
            ckpt_aligning_stopwatch.restart();
    }

    /// When all inputs request checkpoint, propagate the request and reset all checkpoint request
    if (num_requested_checkpoint == input_ports.size())
    {
        std::ranges::for_each(input_ports, [](auto & input) {
            input.requested_checkpoint = false;
            input.status = InputStatus::NeedData;
        });
        num_requested_checkpoint = 0;
        ckpt_aligning_stopwatch.reset();
        last_ckpt_aligning_log_ts = 0;
    }
    else
    {
        input_with_data.status = InputStatus::NotNeedData;
        chunk.clearRequestCheckpoint();
    }
    return true;
}

void ShrinkResizeProcessor::checkAndLogSlowCheckpointAligning()
{
    auto elapsed_ms = ckpt_aligning_stopwatch.elapsedMilliseconds();
    if (elapsed_ms < 1'000)
        return;

    if (auto now = MonotonicMilliseconds::now(); now - last_ckpt_aligning_log_ts >= 5'000)
    {
        auto status = fmt::format(
            "{}",
            fmt::join(
                input_ports | std::views::transform([](const auto & input) { return input.requested_checkpoint ? '1' : '0'; }), ", "));
        LOG_WARNING(
            logger,
            "Slow checkpoint alignment detected({}/{}): [{}] , elapsed {} ms",
            static_cast<uint8_t>(num_requested_checkpoint),
            input_ports.size(),
            status,
            elapsed_ms);
        last_ckpt_aligning_log_ts = now;

        for (auto & input : input_ports)
        {
            if (input.requested_checkpoint)
                input.port->setNotNeeded();
            else
                input.port->setNeeded();
        }
    }
}

ExpandResizeProcessor::ExpandResizeProcessor(const Block & header, size_t num_outputs)
    : IProcessor(InputPorts(1, header), OutputPorts(num_outputs, header), ProcessorID::StreamingExpandResizeProcessorID)
    , header_chunk(outputs.front().getHeader().getColumns(), 0)
{
    assert(num_outputs > 0);
}

IProcessor::Status ExpandResizeProcessor::prepare(const PortNumbers & /*updated_inputs*/, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input.setNeeded();

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    /// Update outputs
    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;

                // Clear exclusive_output if this port was exclusive
                if (exclusive_output.has_value() && *exclusive_output == &output)
                    exclusive_output.reset();
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                if (exclusive_output.has_value() && *exclusive_output == &output)
                {
                    /// Prioritize outputs that require consecutive data
                    waiting_outputs.push_front(*exclusive_output);
                    exclusive_output.reset();
                }
                else
                {
                    waiting_outputs.push_back(&output);
                }
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    /// Check input is finished
    auto & input = inputs.front();
    if (input.isFinished())
    {
        /// Flush all outputs before finish
        bool all_outputs_finished = true;
        for (auto & output : output_ports)
        {
            if (output.port->isFinished())
                continue;

            if (output.port->hasData() || output.propagate_flag)
            {
                all_outputs_finished = false;
                continue;
            }

            output.port->finish();
        }

        if (all_outputs_finished)
            return Status::Finished;
    }

    /// Check input has data
    /// If has checkpoint request, we must waiting for the request is propagated in all outputs before read new data
    if (!waiting_outputs.empty() && num_checkpoint_requests == 0)
    {
        input.setNeeded();

        if (input.hasData() && !exclusive_output.has_value())
        {
            auto data = input.pullData(/*set_not_needed*/ true);
            if (data.chunk.hasWatermark())
            {
                std::ranges::for_each(
                    output_ports, [](auto & output) { output.propagate_flag |= OutputPortWithStatus::PROPAGATE_WATERMARK; });
                watermark = std::max(watermark, data.chunk.getWatermark());
            }
            else if (data.chunk.requestCheckpoint())
            {
                std::ranges::for_each(
                    output_ports, [](auto & output) { output.propagate_flag |= OutputPortWithStatus::PROPAGATE_CHECKPOINT_REQUEST; });
                ckpt_ctx = data.chunk.getCheckpointContext();
                num_checkpoint_requests = output_ports.size();
            }
            else if (!data.chunk.hasRows())
            {
                std::ranges::for_each(
                    output_ports, [](auto & output) { output.propagate_flag |= OutputPortWithStatus::PROPAGATE_HEARTBEAT; });
            }

            if (data.chunk.hasRows())
            {
                assert(num_checkpoint_requests == 0);
                auto & waiting_output = *waiting_outputs.front();
                waiting_outputs.pop_front();
                auto bytes = data.chunk.bytes();
                auto rows = data.chunk.rows();
                auto start_ns = MonotonicNanoseconds::now();

                /// Received consecutive data, make this output exclusive
                if (data.chunk.isConsecutiveData())
                    exclusive_output = &waiting_output;

                waiting_output.port->pushData(std::move(data));
                metrics.processed_bytes += bytes;
                metrics.processed_rows += rows;
                metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
                waiting_output.propagate_flag = OutputPortWithStatus::NO_PROPAGATE;
                waiting_output.status = OutputStatus::NotActive;
            }
        }
    }

    /// Try propagate some context (e.g. watermark/checkpoint or heartbeat)
    for (auto iter = waiting_outputs.begin(); iter != waiting_outputs.end();)
    {
        auto & waiting_output = **iter;
        if (waiting_output.propagate_flag)
        {
            auto chunk = header_chunk.clone();

            /// Checkpoint barrier is always standalone, it can't coexist with watermark, we must propagate watermark first
            if (waiting_output.propagate_flag & OutputPortWithStatus::PROPAGATE_WATERMARK)
            {
                chunk.setWatermark(watermark);
                waiting_output.propagate_flag &= ~(OutputPortWithStatus::PROPAGATE_WATERMARK | OutputPortWithStatus::PROPAGATE_HEARTBEAT);
            }
            else if (waiting_output.propagate_flag & OutputPortWithStatus::PROPAGATE_CHECKPOINT_REQUEST)
            {
                chunk.setCheckpointContext(ckpt_ctx);
                waiting_output.propagate_flag
                    &= ~(OutputPortWithStatus::PROPAGATE_CHECKPOINT_REQUEST | OutputPortWithStatus::PROPAGATE_HEARTBEAT);
                assert(num_checkpoint_requests > 0);
                --num_checkpoint_requests;
            }
            else
            {
                waiting_output.propagate_flag &= ~OutputPortWithStatus::PROPAGATE_HEARTBEAT;
            }

            waiting_output.port->push(std::move(chunk));
            waiting_output.status = OutputStatus::NotActive;
            iter = waiting_outputs.erase(iter);
        }
        else
            ++iter;
    }

    if (!waiting_outputs.empty())
        return Status::NeedData;

    return Status::PortFull;
}

StrictResizeProcessor::StrictResizeProcessor(const Block & header, size_t num_inputs_and_outputs)
    : IProcessor(
          InputPorts(num_inputs_and_outputs, header),
          OutputPorts(num_inputs_and_outputs, header),
          ProcessorID::StreamingStrictResizeProcessorID)
{
    assert(num_inputs_and_outputs > 0);
}

IProcessor::Status StrictResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive, .waiting_output = -1});

        for (UInt64 i = 0; i < input_ports.size(); ++i)
            disabled_input_ports.push(i);

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    std::queue<UInt64> inputs_with_data;

    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;

                waiting_outputs.push(input.waiting_output);
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::NotActive)
            {
                input.status = InputStatus::NotActive;
                inputs_with_data.push(input_number);
            }
        }
    }

    while (!inputs_with_data.empty())
    {
        auto input_number = inputs_with_data.front();
        auto & input_with_data = input_ports[input_number];
        inputs_with_data.pop();

        if (input_with_data.waiting_output == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data.");

        auto & waiting_output = output_ports[input_with_data.waiting_output];

        if (waiting_output.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output.");

        if (waiting_output.status != OutputStatus::Finished)
        {
            auto data = input_with_data.port->pullData(/* set_not_needed = */ true);
            auto bytes = data.chunk.bytes();
            auto rows = data.chunk.rows();
            auto start_ns = MonotonicNanoseconds::now();
            waiting_output.port->pushData(std::move(data));
            metrics.processed_bytes += bytes;
            metrics.processed_rows += rows;
            metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
            waiting_output.status = OutputStatus::NotActive;
        }
        else
            abandoned_chunks.emplace_back(input_with_data.port->pullData(/* set_not_needed = */ true));

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
        else
            disabled_input_ports.push(input_number);
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Process abandoned chunks if any.
    while (!abandoned_chunks.empty() && !waiting_outputs.empty())
    {
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        auto bytes = abandoned_chunks.back().chunk.bytes();
        auto rows = abandoned_chunks.back().chunk.rows();
        auto start_ns = MonotonicNanoseconds::now();
        waiting_output.port->pushData(std::move(abandoned_chunks.back()));
        metrics.processed_bytes += bytes;
        metrics.processed_rows += rows;
        metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
        abandoned_chunks.pop_back();

        waiting_output.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto & input = input_ports[disabled_input_ports.front()];
        disabled_input_ports.pop();

        input.port->setNeeded();
        input.status = InputStatus::NeedData;
        input.waiting_output = waiting_outputs.front();

        waiting_outputs.pop();
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto & output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        if (output.status != OutputStatus::Finished)
            ++num_finished_outputs;

        output.status = OutputStatus::Finished;
        output.port->finish();
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (disabled_input_ports.empty())
        return Status::NeedData;

    return Status::PortFull;
}

}
}
