#pragma once

#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Common/Stopwatch.h>

namespace DB
{

class ThrottlingTransform final : public ExceptionKeepingTransform
{
public:
    using Callback = std::function<void()>;

    ThrottlingTransform(const Block & header, UInt64 limit_, UInt64 interval_ms_, Callback callback_ = nullptr);

    String getName() const override { return "ThrottlingTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;


private:
    Stopwatch timer;
    UInt64 limit{0};
    UInt64 interval_ms{0};
    UInt64 count{0};
    Chunk header_chunk;
    Chunk cur_chunk;

    Callback callback;
};
}
