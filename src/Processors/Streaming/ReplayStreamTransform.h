#pragma once
#include <Core/Block.h>
#include <Processors/ISimpleTransform.h>
#include <base/types.h>

#include <optional>
#include <queue>
namespace DB
{
namespace Streaming
{

class ReplayStreamTransform final : public ISimpleTransform
{
public:
    ReplayStreamTransform(const Block & header, Float32 replay_speed_, Int64 last_sn_, const String & replay_time_col_);
    String getName() const override { return "ReplayStreamTransform"; }
    void work() override;
    void transform(Chunk & input_chunk, Chunk & output_chunk) override;
    void transform(Chunk & chunk) override;

private:

    Float32 replay_speed = 0;
    size_t time_index = 0;
    size_t sn_index = 0;
    /// this shard's last sequence number;
    Int64 last_sn = 0;
    std::optional<Int64> last_batch_time;
    Int64 wait_interval_ms = 0;
    bool enable_replay = true;
    const String replay_time_col;
    std::queue<Chunk> output_chunks;
};
}
}
