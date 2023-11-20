#include <Processors/Transforms/Streaming/ChangelogTransform.h>

/// #include <base/ClockUtils.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/ProtonCommon.h>
/// #include <Common/SipHash.h>
#include <Common/assert_cast.h>
/// #include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_IDENTIFIER;
}

namespace Streaming
{

ChangelogTransform::ChangelogTransform(
    const DB::Block & input_header,
    const DB::Block & output_header,
    std::vector<std::string> key_column_names,
    const std::string & version_column_name)
    : IProcessor({input_header}, {output_header}, ProcessorID::ChangelogTransformID)
/// , source_chunks(metrics)
/// , last_log_ts(MonotonicMilliseconds::now())
///, logger(&Poco::Logger::get("ChangelogTransform"))
{
    /// assert(!key_column_names.empty());
    delta_column_position = input_header.getPositionByName(ProtonConsts::RESERVED_DELTA_FLAG);

    output_column_positions.reserve(output_header.columns());

    for (const auto & col : output_header)
        output_column_positions.push_back(input_header.getPositionByName(col.name));

    assert(output_column_positions.size() <= output_header.columns());

    /// key_column_positions.reserve(key_column_names.size());
    /// for (const auto & key_col : key_column_names)
    ///    key_column_positions.push_back(input_header.getPositionByName(key_col));

    /// if (!version_column_name.empty())
    ///    version_column_position = input_header.getPositionByName(version_column_name);
}

IProcessor::Status ChangelogTransform::prepare()
{
    /// std::scoped_lock lock(mutex);
    /// FIXME, multiple shards / inputs

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
    }

    Status status = Status::NeedData;
    if (!output_chunks.empty())
    {
        /// First drain what we have so far
        input.setNotNeeded();
        status = Status::PortFull;
    }
    else if (input.isFinished())
    {
        output.finish();
        status = Status::Finished;
    }
    else
    {
        assert(!input_data.chunk);

        /// We have drain output_chunks
        input.setNeeded();

        if (input.hasData())
        {
            input_data = input.pullData(/*set_not_needed= */ false);
            if (input_data.exception)
                /// No more data needed. Exception will be thrown (or swallowed) later.
                input.setNotNeeded();

            status = Status::Ready;
        }
        else
            status = Status::NeedData;
    }

    return status;
}

void ChangelogTransform::work()
{
    if (input_data.exception)
        std::rethrow_exception(input_data.exception);

    const auto & chunk = input_data.chunk;
    if (auto ckpt_ctx = chunk.getCheckpointContext(); ckpt_ctx)
    {
        assert(chunk.rows() == 0);
        transformChunk(input_data.chunk);
        return;
    }

    /// Propagate empty chunk since it acts like a heartbeat
    auto rows = chunk.rows();
    if (!rows)
    {
        transformChunk(input_data.chunk);
        return;
    }

    const auto & chunk_columns = input_data.chunk.getColumns();
    const auto & delta_flags = assert_cast<const ColumnInt8 &>(chunk_columns[delta_column_position]->assumeMutableRef()).getData();

    /// Fast path: process all same `_tp_delta` chunk
    if (std::all_of(delta_flags.begin(), delta_flags.end(), [](auto delta) { return delta > 0; }))
    {
        transformChunk(input_data.chunk);
        return;
    }
    else if (std::all_of(delta_flags.begin(), delta_flags.end(), [](auto delta) { return delta < 0; }))
    {
        input_data.chunk.setRetractedDataFlag();
        transformChunk(input_data.chunk);
        return;
    }

    IColumn::Selector selector(rows, 0);
    for (size_t i = 0; auto delta : delta_flags)
        selector[i++] = delta > 0;

    /// FIXME: Consider multiple changelogs processing orders
    /// Group 0: retract. Group 1: update
    std::array<Chunk, 2> chunks;
    for (const auto & col : chunk_columns)
    {
        auto split_cols = col->scatter(2, selector);
        assert(split_cols.size() == 2);

        for (size_t chunk_index = 0; chunk_index < 2; ++chunk_index)
            chunks[chunk_index].addColumn(std::move(split_cols[chunk_index]));
    }

    if (chunks[0].getNumRows())
    {
        chunks[0].setRetractedDataFlag();
        transformChunk(chunks[0]);
    }

    chunks[1].setChunkContext(input_data.chunk.getChunkContext());
    transformChunk(chunks[1]);

    input_data.chunk.clear();
}

void ChangelogTransform::transformChunk(Chunk & chunk)
{
    Columns output_columns;
    output_columns.reserve(output_chunk_header.getNumColumns());

    auto rows = chunk.rows();
    auto columns = chunk.detachColumns();
    for (auto pos : output_column_positions)
        output_columns.push_back(std::move(columns[pos]));

    chunk.setColumns(std::move(output_columns), rows);

    output_chunks.push_back(std::move(chunk));
}
}
}