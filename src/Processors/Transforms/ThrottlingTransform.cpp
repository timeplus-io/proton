#include <Processors/Transforms/ThrottlingTransform.h>
/// proton: starts
#include <base/ClockUtils.h>
/// proton: ends

namespace DB
{

ThrottlingTransform::ThrottlingTransform(const Block & header, UInt64 limit_, UInt64 interval_ms_, ThrottlingTransform::Callback callback_)
    : ExceptionKeepingTransform(header, header, false, ProcessorID::ThrottlingTransformID)
    , limit(limit_)
    , interval_ms(interval_ms_)
    , callback(std::move(callback_))
{
    header_chunk = Chunk(header.getColumns(), 0);
}

void ThrottlingTransform::onConsume(Chunk chunk)
{
    /// proton: starts. Measure rows/bytes/time for throttling
    auto start_ns = MonotonicNanoseconds::now();
    auto in_rows = chunk.getNumRows();
    auto in_bytes = chunk.bytes();
    if (chunk.rows() == 0)
    {
        cur_chunk = header_chunk.clone();
        metrics.processed_rows += in_rows;
        metrics.processed_bytes += in_bytes;
        metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
        return;
    }

    if (count >= limit)
    {
        if (timer.elapsedMilliseconds() < interval_ms)
        {
            if (callback)
                callback();

            cur_chunk = header_chunk.clone();
            metrics.processed_rows += in_rows;
            metrics.processed_bytes += in_bytes;
            metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
            return;
        }

        count = 0;
        timer.restart();
    }

    auto rows = chunk.rows();
    cur_chunk.setColumns(chunk.detachColumns(), rows);
    ++count;
    metrics.processed_rows += in_rows;
    metrics.processed_bytes += in_bytes;
    metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns;
    /// proton: ends
}

ThrottlingTransform::GenerateResult ThrottlingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
