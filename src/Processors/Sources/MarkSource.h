#pragma once
#include <Processors/ISource.h>

namespace DB
{

/// Mark source generate an head chunk with mark flag set once
class MarkSource final : public ISource
{
public:
    explicit MarkSource(Block header, UInt64 mark) : ISource(std::move(header), ProcessorID::MarkSourceID), chunk(output.getHeader().getColumns(), 0)
    {
        auto chunk_ctx = std::make_shared<ChunkContext>();
        chunk_ctx->setMark(mark);

        chunk.setChunkContext(std::move(chunk_ctx));
    }

    String getName() const override { return "MarkSource"; }

protected:
    Chunk generate() override
    {
        Chunk return_chunk = std::move(chunk);
        chunk.clear();
        return return_chunk;
    }

private:
    Chunk chunk;
};

}
