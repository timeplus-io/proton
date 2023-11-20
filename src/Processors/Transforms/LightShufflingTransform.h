#pragma once

#include <Processors/IProcessor.h>
#include <Processors/LightChunkSplitter.h>

#include <queue>

namespace DB
{
/// LightShufflingTransform shuffle data from one input to N outputs
/// according to keys. Data with the same key is guaranteed to push to
/// the same outputs
class LightShufflingTransform final : public IProcessor
{
public:
    LightShufflingTransform(Block header_, size_t num_outputs_, std::vector<size_t> key_positions_);

    String getName() const override { return "LightShufflingTransform"; }

    Status prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs) override;

    void work() override;

protected:
    void consume(Chunk chunk);

private:
    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<OutputPortWithStatus> output_ports;
    std::list<UInt64> waiting_outputs;
    size_t num_finished_outputs = 0;

    std::vector<std::queue<Chunk>> shuffled_output_chunks;
    LightChunkSplitter chunk_splitter;
    Chunk current_chunk;
};

UInt16 bestTotalOutputStreams(size_t num_output_streams);

}
