#include "ShufflingTransform.h"

#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{

ShufflingTransform::ShufflingTransform(Block header_, size_t num_inputs_, size_t num_outputs_, std::vector<size_t> key_positions_)
    : IProcessor(InputPorts{num_inputs_, header_}, OutputPorts(num_outputs_, header_), ProcessorID::ShufflingTransformID)
    , num_outputs(num_outputs_)
    , shuffled_output_chunks(num_outputs)
    , chunk_splitter(std::move(key_positions_))
{
}

IProcessor::Status ShufflingTransform::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        input_ports.reserve(inputs.size());
        for (auto & input : inputs)
        {
            input.setNeeded();
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive});
        }

        output_ports.reserve(outputs.size());
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
                waiting_outputs.push_back(output_number);
            }
        }
    }

    if (num_finished_outputs == num_outputs)
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

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
                inputs_with_data.push(input_number);
            }
        }
    }

    bool has_output = false;
    for (auto iter = waiting_outputs.begin(); iter != waiting_outputs.end();)
    {
        auto output_port_index = *iter;
        auto & output = output_ports[output_port_index];
        auto & shuffled_bucket = shuffled_output_chunks[output_port_index];

        /// If no data, check next
        if (shuffled_bucket.empty())
        {
            ++iter;
            continue;
        }

        output.port->push(std::move(shuffled_bucket.front()));
        output.status = OutputStatus::NotActive;
        shuffled_bucket.pop();
        iter = waiting_outputs.erase(iter);

        has_output = true;
    }

    if (has_output)
        return Status::PortFull;

    /// Check can input.
    if (!current_chunk)
    {
        if (inputs_with_data.empty())
        {
            if (num_finished_inputs == inputs.size())
            {
                for (auto & output : outputs)
                    output.finish();

                return Status::Finished;
            }

            return Status::NeedData;
        }

        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        current_chunk = input_with_data.port->pull();
        input_with_data.status = InputStatus::NotActive;
    }

    return Status::Ready;
}

void ShufflingTransform::work()
{
    auto start_ns = MonotonicNanoseconds::now();
    {
        metrics.processed_bytes += current_chunk.bytes();

        consume(std::move(current_chunk));
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void ShufflingTransform::consume(Chunk chunk)
{
    if (chunk.hasRows())
    {
        auto split_chunks{chunk_splitter.split(chunk)};
        for (auto & chunk_with_id : split_chunks)
        {
            assert(chunk_with_id.chunk);
            auto output_idx = chunk_with_id.id.items[0] % num_outputs;
            /// Keep substream id for each sub-chunk, used for downstream processors
            chunk_with_id.chunk.getOrCreateChunkContext()->setSubstreamID(std::move(chunk_with_id.id));
            shuffled_output_chunks[output_idx].push(std::move(chunk_with_id.chunk));
        }
    }
    else
    {
        /// FIXME: for an empty chunk, we still transform to each output, only used for global aggregation watermark
        for (size_t output_idx = 0; output_idx < num_outputs; ++output_idx)
            shuffled_output_chunks[output_idx].push(chunk.clone());
    }
}

}
}
