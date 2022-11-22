#include "SessionTransformWithSubstream.h"
#include "Sessionizer.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{
namespace Streaming
{
SessionTransformWithSubstream::SessionTransformWithSubstream(
    const Block & input_header, const Block & output_header, FunctionDescriptionPtr desc_, std::vector<size_t> key_column_positions)
    : IProcessor({input_header}, {output_header}, ProcessorID::SessionTransformWithSubstreamID)
    , desc(std::move(desc_))
    , substream_splitter(std::move(key_column_positions))
    , time_col_is_datetime64(isDateTime64(desc->argument_types[0]))
    , time_col_pos(input_header.getPositionByName(desc->argument_names[0]))
{
}

IProcessor::Status SessionTransformWithSubstream::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.
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

    /// Has substream chunk(s) that needs to output
    if (output_iter != output_chunks.end())
    {
        output.push(std::move(*output_iter));
        ++output_iter;
        return Status::PortFull;
    }

    /// Check can input.
    if (!input_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        input_chunk = input.pull(true);
    }

    /// Now consume.
    return Status::Ready;
}

void SessionTransformWithSubstream::work()
{
    /// the SessionAggregatingTransform needs current event time bound to check oversize session
    /// So we add an empty chunk with min/max event time

    /// We will need clear input_chunk for next run
    Chunk process_chunk;
    process_chunk.swap(input_chunk);

    Chunk min_max_chunk;
    if (process_chunk.hasRows())
    {
        auto min_max_ts = calcMinMaxEventTime(process_chunk);

        auto chunk_ctx = std::make_shared<ChunkContext>();
        chunk_ctx->setWatermark(min_max_ts.second, min_max_ts.first);

        Chunk chunk(getOutputs().front().getHeader().getColumns(), 0);
        chunk.setChunkContext(std::move(chunk_ctx));
        min_max_chunk.swap(chunk);
    }

    auto split_chunks{substream_splitter.split(process_chunk)};

    assert(output_iter == output_chunks.end());
    output_chunks.clear();
    output_chunks.reserve(split_chunks.size());

    for (auto & chunk_with_id : split_chunks)
    {
        auto & sessionizer = getOrCreateSubstreamSessionizer(chunk_with_id.id);

        assert(chunk_with_id.chunk);
        sessionizer.sessionize(chunk_with_id.chunk);
        assert(chunk_with_id.chunk);

        /// Keep substream id for each sub-chunk, used for downstream processors
        auto chunk_ctx = chunk_with_id.chunk.getOrCreateChunkContext();
        chunk_ctx->setSubstreamID(std::move(chunk_with_id.id));

        output_chunks.emplace_back(std::move(chunk_with_id.chunk));
    }

    if (min_max_chunk)
        output_chunks.emplace_back(std::move(min_max_chunk));

    output_iter = output_chunks.begin(); /// need to output chunks
}

std::pair<Int64, Int64> SessionTransformWithSubstream::calcMinMaxEventTime(const Chunk & chunk) const
{
    if (time_col_is_datetime64)
    {
        const auto & time_col = chunk.getColumns()[time_col_pos];
        auto col = assert_cast<ColumnDecimal<DateTime64> *>(time_col->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {min_max_ts.first->value, min_max_ts.second->value};
    }
    else
    {
        const auto & time_col = chunk.getColumns()[time_col_pos];
        auto col = assert_cast<ColumnVector<UInt32> *>(time_col->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {*min_max_ts.first, *min_max_ts.second};
    }
}

Sessionizer & SessionTransformWithSubstream::getOrCreateSubstreamSessionizer(const SubstreamID & id)
{
    auto iter = substream_sessionizers.find(id);
    if (iter == substream_sessionizers.end())
    {
        auto sessionizer = std::make_unique<Sessionizer>(
            inputs.front().getHeader(), outputs.front().getHeader(), desc->session_start, desc->session_end);

        return *(substream_sessionizers.emplace(id, std::move(sessionizer)).first->second);
    }

    return *(iter->second);
}

void SessionTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    (void)ckpt_ctx;
}

void SessionTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    (void)ckpt_ctx;
}

}
}
