#pragma once

#include <Common/Streaming/Substream/Executor.h>

namespace DB::Streaming::Substream
{
class BlockSplitter final : public Executor<BlockSplitter>
{
private:
    using Executor = Executor<BlockSplitter>;
    using DataWithIDRows = typename Executor::DataWithIDRows;

public:
    BlockSplitter(Block header_, std::vector<size_t> keys_indices_) : Executor(std::move(header_), std::move(keys_indices_)) { }

    String getName() const { return "BlockSplitter"; }

    std::vector<std::pair<SubstreamID, Block>> operator()(const Block & block) { return split(block); }

    std::vector<std::pair<SubstreamID, Block>> split(const Block & block)
    {
        std::vector<std::pair<SubstreamID, Block>> result;
        Executor::execute(block, [&](DataWithIDRows & data, const Columns & columns) {
            /// NOTICE: We must save materialized id.
            result.emplace_back(data.id.materialize(), table().header.cloneWithColumns(columns));
        });
        return result;
    }
};
}
