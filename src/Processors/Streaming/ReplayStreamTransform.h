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

class ReplayStreamTransform final : public IProcessor
{
public:
    ReplayStreamTransform(const Block & header, Float32 replay_speed_, Int64 last_sn_, const String & replay_time_col_, std::optional<String> start_time = std::nullopt, std::optional<String> end_time = std::nullopt);
    String getName() const override { return "ReplayStreamTransform"; }
    void work() override;
    Status prepare() override;
    OutputPort & getOutputPort() { return outputs.front(); }

private:
    /// If there are events of different times in the chunk, split them into different chunks
    std::queue<Chunk> splitChunkByTime();

    /// Waiting for the first chunk of \chunks_to_replay to be replay via \replay_clock
    Chunk replayOneChunk();

    Chunk input_chunk;
    Chunk output_chunk;

    /// Set input port NotNeeded after chunk was pulled.
    /// Input port will become needed again only after data was transformed.
    /// This allows to escape caching chunks in input port, which can lead to uneven data distribution.

    const String replay_time_col;
    UInt8 time_scale = 0;
    Float32 replay_speed = 0;
    size_t time_index = 0;
    size_t sn_index = 0;

    /// this shard's last sequence number;
    Int64 last_sn = 0;
    std::optional<Int64> last_batch_time;
    std::optional<Int64> end_time;
    Int64 replay_clock = -1;

    bool initialized = false;
    bool replay_finished = false;
    bool reach_end_time = false;

    std::queue<Chunk> chunks_to_replay;

    Int64 last_log_ts = 0;
    Int64 last_unordered_log_ts = 0;
    Poco::Logger * logger;
};
}
}
