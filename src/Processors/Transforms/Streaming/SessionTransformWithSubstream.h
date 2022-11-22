#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Streaming/ChunkSplitter.h>
#include <Interpreters/Streaming/FunctionDescription.h>

namespace DB
{
namespace Streaming
{
class Sessionizer;

class SessionTransformWithSubstream final : public IProcessor
{
public:
    SessionTransformWithSubstream(
        const Block & input_header,
        const Block & output_header,
        FunctionDescriptionPtr desc_,
        std::vector<size_t> key_column_positions);

    String getName() const override { return "SessionTransformWithSubstream"; }

    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr) override;
    void recover(CheckpointContextPtr) override;

private:
    std::pair<Int64, Int64> calcMinMaxEventTime(const Chunk & chunk) const;
    Sessionizer & getOrCreateSubstreamSessionizer(const SubstreamID & id);

private:
    FunctionDescriptionPtr desc;

    Chunk input_chunk;
    Chunks output_chunks;
    typename Chunks::iterator output_iter{output_chunks.begin()};

    ChunkSplitter substream_splitter;

    SubstreamHashMap<std::unique_ptr<Sessionizer>> substream_sessionizers;

    bool time_col_is_datetime64;
    size_t time_col_pos;
};
}
}
