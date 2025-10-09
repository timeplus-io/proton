#pragma once

#include <Interpreters/SquashingTransform.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Common/Stopwatch.h>

namespace DB
{

class BatchingTransform final : public ExceptionKeepingTransform
{
public:
    BatchingTransform(const Block & header, size_t batch_size_rows, UInt64 timeout_ms_, bool reserve_memory = false);

    String getName() const override { return "BatchingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;

private:
    SquashingTransform squashing;
    Stopwatch timer;
    UInt64 timeout_ms;
    Chunk cur_chunk;
    Chunk finish_chunk;
};
}
