#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/IProcessor.h>

namespace DB
{
class NotJoinedBlocks;

namespace Streaming
{
/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently
/// left stream -> ... ->
///                      \
///                      JoinTransform
///                      /
/// right stream -> ... ->
class JoinTransform final : public IProcessor
{
public:
    JoinTransform(
        Block left_input_header,
        Block right_input_header,
        Block output_header,
        HashJoinPtr join_,
        size_t max_block_size_,
        UInt64 join_max_cached_bytes_);

    String getName() const override { return "StreamingJoinTransform"; }
    Status prepare() override;
    void work() override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    using Chunks = std::array<Chunk, 2>;
    void propagateWatermark(int64_t local_watermark_lower_bound, int64_t local_watermark_upper_bound);
    bool setupWatermark(Chunk & chunk, int64_t local_watermark_lower_bound, int64_t local_watermark_upper_bound);

    void doJoin(Chunks chunks);
    void joinBidirectionally(Chunks chunks);
    void rangeJoinBidirectionally(Chunks chunks);

private:
    struct InputPortWithData
    {
        explicit InputPortWithData(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        Chunk input_chunk;
    };

    /// std::atomic_bool stop_reading = false;
    [[maybe_unused]] bool process_non_joined = true;

    HashJoinPtr join;
    bool range_bidirectional_hash_join = false;
    bool bidirectional_hash_join = false;

    [[maybe_unused]] std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
    [[maybe_unused]] size_t max_block_size;

    Chunk output_header_chunk;

    Poco::Logger * logger;

    mutable std::mutex mutex;

    std::array<InputPortWithData, 2> input_ports_with_data;
    std::list<Chunk> output_chunks;

    int64_t watermark_lower_bound = std::numeric_limits<int64_t>::min();
    int64_t watermark_upper_bound = std::numeric_limits<int64_t>::min();
};
}
}
