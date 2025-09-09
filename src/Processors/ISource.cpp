#include <Processors/ISource.h>
#include <QueryPipeline/StreamLocalLimits.h>

/// proton: starts
#include <IO/Progress.h>
#include <base/ClockUtils.h>
#include <Common/ProfileEvents.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::~ISource() = default;

ISource::ISource(Block header, bool enable_auto_progress, ProcessorID pid_)
    : IProcessor({}, {std::move(header)}, pid_)
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

ISource::Status ISource::prepare()
{
    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    std::lock_guard lock(read_progress_mutex);
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    if (finished && read_progress.read_bytes == 0 && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void ISource::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void ISource::addTotalBytes(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_bytes += value;
}

void ISource::work()
{
    try
    {
        /// proton: starts.
        auto start_ns = MonotonicNanoseconds::now();
        /// proton: ends.

        read_progress_was_set = false;

        if (auto chunk = tryGenerate())
        {
            /// proton: starts.
            metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
            const auto in_bytes = chunk->bytes();
            const auto in_rows = chunk->rows();
            metrics.processed_bytes += in_bytes;
            metrics.processed_rows += in_rows;
            /// proton: ends.

            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                {
                    /// Reuse in_rows/in_bytes we just computed for metrics
                    progress(in_rows, in_bytes);
                    if (progress_callback)
                        progress_callback(Progress(DB::ReadProgress(in_rows, in_bytes)));
                }
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (const std::exception & ex)
    {
        finished = true;
        /// proton: starts
        setLastErrorMessage(ex.what());
        /// proton: ends
        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}

}
