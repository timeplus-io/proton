#pragma once
#include <optional>
#include <Processors/ISimpleTransform.h>
#include "Core/Block.h"
#include "base/types.h"


namespace DB
{
namespace Streaming
{
class ReplayStreamTransform final : public ISimpleTransform
{
public:
    ReplayStreamTransform(const Block & header, Float32 replay_speed_);
    String getName() const override { return "ReplayStreamTransform"; }
    void transform(Chunk & chunk) override;

private:
    Float32 replay_speed = 0;
    size_t append_time_index = 0;

    std::optional<Int64> last_batch_time;
    Int64 wait_interval = 0;
    /// TODO：mark the historical end
    /// bool begin_stream = false;
};
}
}
