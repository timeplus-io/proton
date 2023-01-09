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

ShufflingTransform::ShufflingTransform(Block header_, size_t num_outputs_, std::vector<size_t> key_positions_)
    : IProcessor(InputPorts{1, header_}, OutputPorts(num_outputs_, header_), ProcessorID::ShufflingTransformID)
    , shuffled_output_chunks(num_outputs_)
    , chunk_splitter(std::move(key_positions_))
{
    output_ports.reserve(outputs.size());
    for (auto & output : outputs)
        output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
}

IProcessor::Status ShufflingTransform::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
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

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
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
        auto & input = inputs.front();
        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_chunk = input.pull();
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
            auto output_idx = chunk_with_id.id.items[0] % outputs.size();
            /// Keep substream id for each sub-chunk, used for downstream processors
            chunk_with_id.chunk.getOrCreateChunkContext()->setSubstreamID(std::move(chunk_with_id.id));
            shuffled_output_chunks[output_idx].push(std::move(chunk_with_id.chunk));
        }
    }
    else
    {
        /// FIXME: for an empty chunk, we still transform to each output, only used for global aggregation watermark
        for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx)
            shuffled_output_chunks[output_idx].push(chunk.clone());
    }
}

}
}
