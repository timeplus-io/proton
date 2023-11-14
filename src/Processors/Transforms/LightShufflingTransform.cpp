#include <Processors/Transforms/LightShufflingTransform.h>

#include <base/ClockUtils.h>

#include <bit>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

UInt16 bestTotalOutputStreams(size_t num_output_streams)
{
    auto to_power_of_two = [](UInt32 x) {
        if (x <= 1)
            return 1;

        return static_cast<UInt16>(1) << (32 - std::countl_zero(x - 1));
    };

    return to_power_of_two(std::min<UInt32>(static_cast<UInt32>(num_output_streams), 256));
}

LightShufflingTransform::LightShufflingTransform(Block header_, size_t num_outputs_, std::vector<size_t> key_positions_)
    : IProcessor(InputPorts{1, header_}, OutputPorts(num_outputs_, header_), ProcessorID::LightShufflingTransformID)
    , shuffled_output_chunks(num_outputs_)
    , chunk_splitter(std::move(key_positions_), num_outputs_)
{
    assert(num_outputs_ > 0 && (num_outputs_ & (num_outputs_ - 1)) == 0);

    output_ports.reserve(outputs.size());
    for (auto & output : outputs)
        output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
}

IProcessor::Status LightShufflingTransform::prepare(const PortNumbers &, const PortNumbers & updated_outputs)
{
    /// Find outputs which can accept more data
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

    /// Loop waiting outputs which can accept more data
    for (auto iter = waiting_outputs.begin(); iter != waiting_outputs.end();)
    {
        auto output_port_index = *iter;
        auto & shuffled_chunks = shuffled_output_chunks[output_port_index];

        /// If no buffered chunk for this waiting output, check next waiting output
        if (shuffled_chunks.empty())
        {
            ++iter;
            continue;
        }

        auto & waiting_output = output_ports[output_port_index];
        waiting_output.port->push(std::move(shuffled_chunks.front()));
        shuffled_chunks.pop();

        waiting_output.status = OutputStatus::NotActive;
        iter = waiting_outputs.erase(iter);
    }

    auto drained = std::all_of(
        shuffled_output_chunks.begin(), shuffled_output_chunks.end(), [](const auto & shuffled_chunks) { return shuffled_chunks.empty(); });

    /// We choose to drain all of the buffered / shuffled chunks to avoid buffer too many shuffled chunks
    if (!drained)
        return Status::PortFull;

    /// Only when buffered chunk are drained, then check if we can pull more data in from input port.
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

void LightShufflingTransform::work()
{
    auto start_ns = MonotonicNanoseconds::now();
    {
        metrics.processed_bytes += current_chunk.bytes();

        consume(std::move(current_chunk));
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void LightShufflingTransform::consume(Chunk chunk)
{
    if (chunk.hasRows())
    {
        auto split_chunks{chunk_splitter.split(chunk)};
        for (auto & chunk_with_shard : split_chunks)
        {
            assert(chunk_with_shard.chunk);
            shuffled_output_chunks[chunk_with_shard.shard].push(std::move(chunk_with_shard.chunk));
        }
    }
    else
    {
        /// Shuffling is upstream, downstream watermark transform still
        /// depends on this empty timer chunk to calculate watermark for global aggregation
        /// When we fix the timer issue systematically, the pipeline system shall have minimum
        /// empty block flowing around and we don't need this anymore
        for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx)
            shuffled_output_chunks[output_idx].push(chunk.clone());
    }
}

}
