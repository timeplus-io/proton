#pragma once

#include <Core/Names.h>
#include <Processors/IProcessor.h>
#include <Processors/Streaming/SubstreamChunkSplitter.h>
#include <Common/Logger.h>

#include <queue>


namespace DB
{
namespace Streaming
{

/// SubstreamShufflingTransform shuffle data from one input to N outputs
/// according to keys. Data with the same key is guaranteed to push to
/// the same outputs
class SubstreamShufflingTransform final : public IProcessor
{
public:
    SubstreamShufflingTransform(Block header_, size_t num_outputs_, const Names & keys_);

    String getName() const override { return "SubstreamShufflingTransform"; }

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

    std::vector<std::list<Chunk>> shuffled_output_chunks;
    SubstreamChunkSplitter chunk_splitter;
    Chunk current_chunk;

    LoggerPtr logger;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;
};

}
}
