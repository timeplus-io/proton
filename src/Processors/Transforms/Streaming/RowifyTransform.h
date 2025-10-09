#pragma once

#include <Processors/IProcessor.h>

namespace DB::Streaming
{

/// RowifyTransform splits batch input into separate rows output
/// This transform takes a chunk with multiple rows and outputs each row as a separate chunk
class RowifyTransform final : public IProcessor
{
public:
    explicit RowifyTransform(const Block & header);

    String getName() const override { return "RowifyTransform"; }

    Status prepare() override;
    void work() override;

private:
    Chunk input_chunk;
    ChunkList output_chunks;
};

}
