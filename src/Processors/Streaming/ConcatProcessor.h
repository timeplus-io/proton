#pragma once

#include <Processors/IProcessor.h>


namespace DB::Streaming
{

/** Concat historical inputs and streaming inputs with same structure.
  * Only used for backfill concat.
  * Pulls all data from historical inputs, then all data from streaming inputs
  * Doesn't do any heavy calculations.
  */
class ConcatProcessor final : public IProcessor
{
    using ConcatCallback = std::function<void(Int64)>;

public:
    ConcatProcessor(const Block & header, size_t num_historical_inputs_, size_t num_streaming_inputs_, ConcatCallback callback);

    String getName() const override { return "StreamingConcat"; }

    Status prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs) override;

private:
    struct PortsPair
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;
        bool finished = false;
    };

    void finishPair(PortsPair & pair);
    /// Returns true if need more data
    bool processPair(PortsPair & pair);

    size_t num_historical_inputs = 0;

    std::vector<PortsPair> ports_pairs;
    size_t num_finished_pairs = 0;

    struct OutputWithStatus
    {
        OutputPort * output = nullptr;
        std::vector<PortsPair *> historical_inputs{};
        bool finished = false;
    };
    std::vector<OutputWithStatus> outputs_with_statuses;
    size_t num_finished_outputs = 0;

    ConcatCallback concat_callback;
    bool concatenated = false;
    Int64 max_sn = -1;

    bool are_inputs_initialized = false;
};

}
