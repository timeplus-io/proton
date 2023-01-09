#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Streaming/ChunkSplitter.h>

#include <queue>


namespace DB
{
namespace Streaming
{

class ShufflingTransform final : public IProcessor
{
public:
    ShufflingTransform(Block header_, size_t num_outputs_, std::vector<size_t> key_positions_);

    String getName() const override { return "ShufflingTransform"; }

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
    ChunkSplitter chunk_splitter;
    Chunk current_chunk;
};

}
}
