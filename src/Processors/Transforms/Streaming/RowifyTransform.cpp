#include <Processors/Transforms/Streaming/RowifyTransform.h>

namespace DB::Streaming
{
RowifyTransform::RowifyTransform(const Block & header) : IProcessor({header}, {header}, ProcessorID::RowifyTransformID)
{
}

IProcessor::Status RowifyTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Push if we have data
    if (!output_chunks.empty())
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
        return Status::PortFull;
    }

    if (input_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (input.hasData())
    {
        input_chunk = input.pull(/*set_not_needed=*/true);
        return Status::Ready;
    }

    return Status::NeedData;
}

void RowifyTransform::work()
{
    if (!input_chunk.hasRows())
    {
        output_chunks.emplace_back(std::move(input_chunk));
        return;
    }

    auto rows = input_chunk.rows();
    auto columns = input_chunk.detachColumns();
    for (size_t i = 0; i < rows; ++i)
    {
        /// Create a single-row chunk
        Columns result_columns;
        result_columns.reserve(columns.size());

        for (const auto & input_column : columns)
        {
            auto result_column = input_column->cloneEmpty();
            result_column->insertFrom(*input_column, i);
            result_columns.push_back(std::move(result_column));
        }

        /// Create chunk with single row
        output_chunks.emplace_back(std::move(result_columns), 1);
    }

    output_chunks.back().setChunkContext(input_chunk.getChunkContext());
    input_chunk.clear();
}

}
