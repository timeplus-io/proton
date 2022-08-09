#pragma once
#include <Processors/ISource.h>

namespace DB
{

/// Mark source generate an head chunk with mark flag set once
class MarkSource : public ISource
{
public:
    explicit MarkSource(Block header, UInt64 mark) : ISource(std::move(header)), chunk(output.getHeader().getColumns(), 0)
    {
        auto chunk_info = std::make_shared<ChunkInfo>();
        chunk_info->ctx.setMark(mark);

        chunk.setChunkInfo(std::move(chunk_info));
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
