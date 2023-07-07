#include <Processors/Streaming/OffsetTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
OffsetTransform::OffsetTransform(const Block & header_, UInt64 offset_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_), ProcessorID::StreamingOffsetTransformID)
    , offset(offset_)
{
    ports_data.resize(num_streams);

    size_t cur_stream = 0;
    for (auto & input : inputs)
    {
        ports_data[cur_stream].input_port = &input;
        ++cur_stream;
    }

    cur_stream = 0;
    for (auto & output : outputs)
    {
        ports_data[cur_stream].output_port = &output;
        ++cur_stream;
    }
}


IProcessor::Status OffsetTransform::prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;

    auto process_pair = [&](size_t pos) {
        auto status = preparePair(ports_data[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished: {
                if (!ports_data[pos].is_finished)
                {
                    ports_data[pos].is_finished = true;
                    ++num_finished_port_pairs;
                }

                return;
            }
            case IProcessor::Status::PortFull: {
                has_full_port = true;
                return;
            }
            case IProcessor::Status::NeedData:
                return;
            /// proton: starts.
            case IProcessor::Status::Ready: {
                auto & port_data = ports_data[pos];
                assert(port_data.input_port->getOutputPort().getProcessor().isStreaming());
                if (!port_data.request_checkpoint)
                {
                    port_data.request_checkpoint = true;
                    ++curr_num_requested_checkpoint;
                }
                return;
            }
            /// proton: ends.
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected status for OffsetTransform::preparePair : {}", IProcessor::statusToName(status));
        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (num_finished_port_pairs == ports_data.size())
        return Status::Finished;

    /// proton: starts.
    /// Propagate checkpoint request
    for (auto & port_data : ports_data)
    {
        if (port_data.current_chunk && !port_data.request_checkpoint)
        {
            assert(port_data.output_port->canPush());
            port_data.output_port->push(std::move(port_data.current_chunk));
            has_full_port = true;
        }
    }

    /// All streaming ports are requested checkpoint
    if (curr_num_requested_checkpoint == ports_data.size())
        return Status::Ready;
    /// proton: ends.

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

OffsetTransform::Status OffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception("prepare without arguments is not supported for multi-port OffsetTransform", ErrorCodes::LOGICAL_ERROR);

    return prepare({0}, {0});
}

OffsetTransform::Status OffsetTransform::preparePair(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can output.
    bool output_finished = false;
    if (output.isFinished())
    {
        output_finished = true;
    }

    if (!output_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull(true);

    /// proton: starts. Ready to do checkpoint
    if (data.current_chunk.requestCheckpoint())
    {
        input.setNotNeeded();
        return Status::Ready;
    }
    /// proton: ends.

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Process block.

    rows_read += rows;

    if (rows_read <= offset)
    {
        data.current_chunk.clear();

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    if (!(rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows))
        splitChunk(data);

    output.push(std::move(data.current_chunk));

    return Status::PortFull;
}


void OffsetTransform::splitChunk(PortsData & data) const
{
    UInt64 num_rows = data.current_chunk.getNumRows();
    UInt64 num_columns = data.current_chunk.getNumColumns();

    /// return a piece of the block
    UInt64 start = 0;

    /// ------------[....(.....]
    /// <----------------------> rows_read
    ///             <----------> num_rows
    /// <---------------> offset
    ///             <---> start

    assert(offset < rows_read);

    if (offset + num_rows > rows_read)
        start = offset + num_rows - rows_read;
    else
        return;

    UInt64 length = num_rows - start;

    auto columns = data.current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    data.current_chunk.setColumns(std::move(columns), length);
}

/// proton: starts.
void OffsetTransform::work()
{
    auto start_ns = MonotonicNanoseconds::now();

    auto & any_port_data = ports_data.front();
    assert(any_port_data.request_checkpoint);
    assert(any_port_data.current_chunk.requestCheckpoint());

    checkpoint(any_port_data.current_chunk.getCheckpointContext());

    /// Reset checkpoint status
    curr_num_requested_checkpoint = 0;
    for (auto & port_data : ports_data)
        port_data.request_checkpoint = false;

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void OffsetTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        writeIntBinary(rows_read, wb);

        bool has_applied_limit = rows_before_limit_at_least && rows_before_limit_at_least->hasAppliedLimit();
        writeBoolText(has_applied_limit, wb);
        if (has_applied_limit)
        {
            uint64_t rows_before_limit = rows_before_limit_at_least->get();
            writeIntBinary(rows_before_limit, wb);
        }
    });
}

void OffsetTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
        readIntBinary(rows_read, rb);

        bool has_applied_limit;
        readBoolText(has_applied_limit, rb);
        if (has_applied_limit)
        {
            if (!rows_before_limit_at_least)
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover streaming limit checkpoint. The rows_before_limit_at_least are not the same, checkpointed: {}, but "
                    "current: {}",
                    has_applied_limit,
                    false);

            uint64_t rows_before_limit;
            readIntBinary(rows_before_limit, rb);
            rows_before_limit_at_least->set(rows_before_limit);
        }
    });
}
/// proton: ends.

}
}
