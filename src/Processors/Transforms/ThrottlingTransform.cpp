#include <Processors/Transforms/ThrottlingTransform.h>

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
    if (chunk.rows() == 0)
    {
        cur_chunk = header_chunk.clone();
        return;
    }

    if (count >= limit)
    {
        if (timer.elapsedMilliseconds() < interval_ms)
        {
            if (callback)
                callback();

            cur_chunk = header_chunk.clone();
            return;
        }

        count = 0;
        timer.restart();
    }

    auto rows = chunk.rows();
    cur_chunk.setColumns(chunk.detachColumns(), rows);
    ++count;
}

ThrottlingTransform::GenerateResult ThrottlingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
