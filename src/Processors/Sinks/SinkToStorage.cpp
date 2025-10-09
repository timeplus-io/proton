#include <DataTypes/NestedUtils.h>
#include <IO/Progress.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

/// proton: starts
namespace ErrorCodes
{
extern const int NOT_A_LEADER;
extern const int STREAM_PAUSED;
extern const int TOO_LARGE_RECORD;
extern const int UNKNOWN_STREAM;
}
/// proton: ends

SinkToStorage::SinkToStorage(const Block & header, ProcessorID pid_) : ExceptionKeepingTransform(header, header, false, pid_) {}

void SinkToStorage::onConsume(Chunk chunk)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(getHeader().cloneWithColumns(chunk.getColumns()));

    consume(chunk.clone());

    /// Process progress if consumption succeeded
    if (progress_callback)
        progress_callback(Progress(WriteProgress(chunk.getNumRows(), chunk.bytes())));

    if (!lastBlockIsDuplicate())
        cur_chunk = std::move(chunk);
}

SinkToStorage::GenerateResult SinkToStorage::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

/// proton: starts
bool SinkToStorage::isErrorRetryable(int error_code)
{
    static std::unordered_set non_retryable_errors = {
        ErrorCodes::UNKNOWN_STREAM,
        ErrorCodes::STREAM_PAUSED,
        ErrorCodes::TOO_LARGE_RECORD,
        ErrorCodes::NOT_A_LEADER,
    };

    return !non_retryable_errors.contains(error_code);
}
/// proton: ends

}
