#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/Streaming/StreamingHashJoin.h>

namespace DB
{
using StreamingHashJoinPtr = std::shared_ptr<StreamingHashJoin>;

class NotJoinedBlocks;

/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// left stream -> ... ->
///                     \
///                     StreamingJoinTransform
///                     /
/// left stream -> ... ->
class StreamingJoinTransform final : public IProcessor
{
public:
    /// Count streams and check which is last.
    /// The last one should process non-joined rows.
    class FinishCounter
    {
    public:
        explicit FinishCounter(size_t total_) : total(total_) {}

        bool isLast()
        {
            return finished.fetch_add(1) + 1 >= total;
        }

    private:
        const size_t total;
        std::atomic<size_t> finished{0};
    };

    using FinishCounterPtr = std::shared_ptr<FinishCounter>;

    StreamingJoinTransform(
        Block left_input_header,
        Block right_input_header,
        StreamingHashJoinPtr join_,
        size_t max_block_size_,
        UInt64 join_max_wait_ms_,
        UInt64 join_max_wait_rows_,
        UInt64 join_max_cached_bytes_,
        FinishCounterPtr finish_counter_ = nullptr);

    String getName() const override { return "StreamingJoinTransform"; }
    Status prepare() override;
    void work() override;

    static Block transformHeader(Block header, const StreamingHashJoinPtr & join);

private:
    bool timeToJoin() const;
    void validateAsofJoinKey(const Block & left_input_header, const Block & right_input_header);

private:

    struct PortContext
    {
        explicit PortContext(InputPort * input_port_) : input_port(input_port_) { }

        InputPort * input_port;
        bool has_input = false;
        Chunk input_chunk;
    };

    std::vector<PortContext> port_contexts;
    std::vector<decltype(&StreamingHashJoin::insertLeftBlock)> join_funcs;
    std::array<std::atomic_bool , 2> port_can_have_more_data;

    Chunk header_chunk;
    mutable std::mutex mutex;
    std::list<Chunk> output_chunks;

    /// std::atomic_bool stop_reading = false;
    [[maybe_unused]] bool process_non_joined = true;

    StreamingHashJoinPtr join;

    /// ExtraBlockPtr left_not_processed;
    /// ExtraBlockPtr right_not_processed;

    FinishCounterPtr finish_counter;
    [[maybe_unused]] std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
    [[maybe_unused]] size_t max_block_size;
    UInt64 join_max_wait_ms;
    UInt64 join_max_wait_rows;
    UInt64 join_max_cached_bytes;

    mutable UInt64 last_join = 0;
    mutable UInt64 added_rows_since_last_join = 0;
};
}
