#include <Processors/Transforms/Streaming/ShufflingTransform.h>

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

IProcessor::Status ShufflingTransform::prepare(const PortNumbers &, const PortNumbers & updated_outputs)
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
        auto & waiting_output = output_ports[output_port_index];
        auto & shuffled_chunks = shuffled_output_chunks[output_port_index];

        /// If no buffered chunk for this waiting output, check next waiting output
        if (shuffled_chunks.empty())
        {
            ++iter;
            continue;
        }

        waiting_output.port->push(std::move(shuffled_chunks.front()));
        waiting_output.status = OutputStatus::NotActive;
        shuffled_chunks.pop();
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
        auto chunk_ctx = chunk.getOrCreateChunkContext(); /// save chunk context before split
        auto split_chunks{chunk_splitter.split(chunk)};
        for (auto & chunk_with_id : split_chunks)
        {
            assert(chunk_with_id.chunk);
            auto output_idx = chunk_with_id.id.items[0] % outputs.size();
            /// Keep substream id for each sub-chunk, used for downstream processors
            auto new_chunk_ctx = ChunkContext::create(*chunk_ctx);
            new_chunk_ctx->setSubstreamID(std::move(chunk_with_id.id));
            chunk_with_id.chunk.setChunkContext(std::move(new_chunk_ctx));
            shuffled_output_chunks[output_idx].push(std::move(chunk_with_id.chunk));
        }
    }
    else
    {
        chunk.trySetSubstreamID(INVALID_SUBSTREAM_ID);

        /// Shuffling is upstream, downstream substream watermark transform still
        /// depends on this empty timer chunk to calculate watermark for global aggregation
        /// When we fix the timer issue systematically, the pipeline system shall have minimum
        /// empty block flowing around and we don't need this anymore
        for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx)
            shuffled_output_chunks[output_idx].push(chunk.clone());
    }
}

}
}
