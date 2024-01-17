#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/IProcessor.h>
#include <base/SerdeTag.h>

namespace DB
{
class NotJoinedBlocks;

namespace Streaming
{
/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently
///         left stream -> ... ->
///                                 \
///                                     JoinTransformWithStaticRightStream
///                                 /
/// static right stream -> ... ->
class JoinTransformWithStaticRightStream final : public IProcessor
{
public:
    JoinTransformWithStaticRightStream(
        Block left_input_header, Block right_input_header, Block output_header, HashJoinPtr join_, UInt64 join_max_cached_bytes_);

    String getName() const override { return "StreamingJoinTransformWithStaticRightStream"; }
    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformHeader(Block header, const HashJoinPtr & join);

private:
    void onCancel() override;

private:
    SERDE HashJoinPtr join;

    Chunk join_chunk;
    Chunk filling_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    bool required_consecutive_filling = false;

    Chunk output_header_chunk;

    Poco::Logger * logger;
};
}
}
