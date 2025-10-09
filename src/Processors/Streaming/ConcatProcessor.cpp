#include <Processors/Streaming/ConcatProcessor.h>

#include <Common/logger_useful.h>

namespace DB::Streaming
{
ConcatProcessor::ConcatProcessor(const Block & header, size_t num_historical_inputs_, size_t num_streaming_inputs_, ConcatCallback callback)
    : IProcessor(
          InputPorts(num_historical_inputs_ + num_streaming_inputs_, header),
          OutputPorts{num_streaming_inputs_, header},
          ProcessorID::StreamingConcatProcessorID)
    , num_historical_inputs(num_historical_inputs_)
    , concat_callback(std::move(callback))
{
    chassert(inputs.size() > outputs.size());
    ports_pairs.reserve(inputs.size());
    auto input_it = inputs.begin();
    auto output_it = outputs.begin();
    /// Prepare pairs of historical inputs
    for (size_t i = 0; i < num_historical_inputs; ++i)
    {
        ports_pairs.push_back(PortsPair{.input = &(*input_it++), .output = &(*output_it++)});
        if (output_it == outputs.end())
            output_it = outputs.begin();
    }

    /// Prepare pairs of streaming inputs
    for (auto & output : outputs)
        ports_pairs.push_back(PortsPair{.input = &(*input_it++), .output = &output});

    /// Prepare outputs with statuses
    outputs_with_statuses.reserve(outputs.size());
    for (auto & output : outputs)
        outputs_with_statuses.emplace_back(OutputWithStatus{.output = &output});

    for (size_t i = 0; i < num_historical_inputs; ++i)
        outputs_with_statuses[i % outputs.size()].historical_inputs.push_back(&ports_pairs[i]);
}

void ConcatProcessor::finishPair(PortsPair & pair)
{
    if (!pair.finished)
    {
        pair.input->close();
        pair.finished = true;
        ++num_finished_pairs;
    }
}

bool ConcatProcessor::processPair(PortsPair & pair)
{
    if (pair.input->isFinished() || pair.output->isFinished())
    {
        finishPair(pair);
        return false;
    }

    if (!pair.output->canPush())
    {
        pair.input->setNotNeeded();
        return false;
    }

    pair.input->setNeeded();
    if (pair.input->hasData())
    {
        /// For streaming inputs (i.e. \concatenated == true), we always pull data as needed
        auto data = pair.input->pullData(/*set_not_needed=*/concatenated);
        if (!concatenated && data.chunk.hasSN())
            max_sn = std::max(max_sn, data.chunk.getSN());

        pair.output->pushData(std::move(data));
        return false;
    }
    return true; /// Need more data
}

IProcessor::Status ConcatProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!are_inputs_initialized)
    {
        /// Close all historical inputs after recovered
        if (concatenated)
        {
            for (size_t i = 0; i < num_historical_inputs; ++i)
                ports_pairs[i].input->close();

            /// Delay activation of streaming inputs until its output requires data
        }
        else
        {
            /// Activate all historical inputs.
            for (size_t i = 0; i < num_historical_inputs; ++i)
                ports_pairs[i].input->setNeeded();
        }

        are_inputs_initialized = true;
    }

    bool need_data = false;
    for (const auto & output_number : updated_outputs)
    {
        auto & output_with_status = outputs_with_statuses[output_number];
        if (output_with_status.output->isFinished())
        {
            if (!output_with_status.finished)
            {
                output_with_status.finished = true;
                ++num_finished_outputs;
            }
            continue;
        }

        if (concatenated)
        {
            auto & streaming_ports_pair = ports_pairs[num_historical_inputs + output_number];
            need_data |= processPair(streaming_ports_pair);
        }
        else
        {
            for (auto * input_ports_pair : output_with_status.historical_inputs)
            {
                need_data |= processPair(*input_ports_pair);
                if (!output_with_status.output->canPush())
                    break;
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    for (const auto & input_number : updated_inputs)
        need_data |= processPair(ports_pairs[input_number]);

    /// Check inputs finished
    if (!concatenated && num_finished_pairs == num_historical_inputs)
    {
        /// All historical inputs are finished, switch to streaming inputs
        concatenated = true;
        num_finished_pairs = 0;
        if (concat_callback)
            concat_callback(max_sn);

        /// Activate streaming inputs
        for (size_t i = num_historical_inputs; i < ports_pairs.size(); ++i)
        {
            auto & streaming_ports_pair = ports_pairs[i];
            if (streaming_ports_pair.output->canPush())
                streaming_ports_pair.input->setNeeded();
        }
        need_data = true;
    }
    else if (concatenated && num_finished_pairs == outputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

}
