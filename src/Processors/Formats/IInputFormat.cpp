#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>

/// proton: starts
#include <thread>
/// proton: ends

namespace DB
{
/// proton: starts
namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}
/// proton: ends

IInputFormat::IInputFormat(Block header, ReadBuffer & in_)
    : ISource(std::move(header)), in(&in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

void IInputFormat::resetParser()
{
    in->ignoreAll();
    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = &in_;
}

/// proton: starts
Block IInputFormat::read(size_t num_rows, UInt64 timeout_ms)
{
    UInt64 total_rows = 0;
    UInt64 read_rows = num_rows;
    UInt64 wait_ms = 0;
    Chunk result;

    while (wait_ms < timeout_ms && ((total_rows < num_rows && read_rows > 0) || (total_rows == 0)))
    {
        auto chunk = generate();
        read_rows = chunk.getNumRows();
        if (!result)
            result = std::move(chunk);
        else
            result.append(chunk);
        total_rows += read_rows;

        if (read_rows == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            wait_ms += 5;
        }
    }

    if (wait_ms >= timeout_ms)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "read block timeout after {} milliseconds.", wait_ms);

    Block block = getPort().getHeader().cloneWithColumns(result.detachColumns());
    /// TODO: handle ChunkInfo for chunk produced by aggregating functions
    return block;
}
/// proton: ends
}
