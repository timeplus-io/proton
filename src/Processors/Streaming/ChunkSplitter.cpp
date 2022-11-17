#include "ChunkSplitter.h"

#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>

namespace DB
{
namespace Streaming
{
/// 1. The ABSL map out-performs std::unordered_map in every single test case
/// 2. ABSL map out-performs internal HashMap in most test cases in chunk_splitter_perf
/// 3. std::find beats almost everybody for every case in chunk_splitter_perf, but one potential problem is when keys are growing, it is not scale
#define USE_ABSL_CHUNK_MAP 1
/// #define USE_STD_CHUNK_MAP 1
/// #define USE_FIND_ALGO 1

ChunkSplitter::ChunkSplitter(std::vector<size_t> key_column_positions_) : key_column_positions(std::move(key_column_positions_))
{
}

std::vector<ChunkWithID> ChunkSplitter::splitOneRow(Chunk & chunk) const
{
    UInt128 key{};
    SipHash hash;

    const auto & columns = chunk.getColumns();
    for (auto position : key_column_positions)
        columns[position]->updateHashWithValue(0, hash);

    hash.get128(key);

    std::vector<ChunkWithID> chunks;
    chunks.reserve(1);
    chunks.emplace_back(std::move(key), std::move(chunk));

    return chunks;
}

std::vector<ChunkWithID> ChunkSplitter::split(Chunk & chunk) const
{
    UInt64 num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return {};

    if (num_rows == 1)
        return splitOneRow(chunk);

    const auto & columns = chunk.getColumns();

    IColumn::Selector selector(num_rows, 0);

    std::vector<ChunkWithID> chunks;
    {
#if defined(USE_ABSL_CHUNK_MAP)
        using ChunkMap = absl::flat_hash_map<UInt128, UInt32, UInt128TrivialHash>;
        ChunkMap selector_buckets(5);
        selector_buckets.reserve(5);
#elif defined(USE_STD_CHUNK_MAP)
        using ChunkMap = std::unordered_map<UInt128, UInt32, UInt128TrivialHash>;
        ChunkMap selector_buckets(5);
#elif defined(USE_FIND_ALGO)
        chunks.reserve(5);
#else
        using ChunkMap = HashMap<UInt128, UInt32, UInt128TrivialHash>;
        ChunkMap selector_buckets(5);
#endif

        for (UInt64 row = 0; row < num_rows; ++row)
        {
            UInt128 key{};
            SipHash hash;

            for (auto position : key_column_positions)
                columns[position]->updateHashWithValue(row, hash);

            hash.get128(key);

#if defined(USE_ABSL_CHUNK_MAP) || defined(USE_STD_CHUNK_MAP)
            auto [iter, inserted] = selector_buckets.try_emplace(std::move(key), selector_buckets.size());
            if (inserted)
            {
                chunks.emplace_back(std::move(key), Chunk{});
                chunks.back().chunk.reserve(columns.size());
            }
            selector[row] = iter->second;
#elif defined(USE_FIND_ALGO)
            auto iter = std::find_if(chunks.begin(), chunks.end(), [&key](const auto & chunk_with_id) { return chunk_with_id.id == key; } );
            if (iter == chunks.end())
            {
                selector[row] = chunks.size();
                chunks.emplace_back(std::move(key), Chunk{});
                chunks.back().chunk.reserve(columns.size());
            }
            else
                selector[row] = std::distance(chunks.begin(), iter);
#else
            ChunkMap::LookupResult it;
            bool inserted;
            selector_buckets.emplace(key, it, inserted);
            if (inserted)
            {
                /// Setup the selector bucket which starts from 0
                new (&it->getMapped()) size_t(selector_buckets.size() - 1);
                chunks.emplace_back(std::move(key), Chunk{});
                chunks.back().chunk.reserve(columns.size());
            }

            selector[row] = it->getMapped();
#endif
        }

    }

    auto num_chunks = chunks.size();
    if (num_chunks == 1)
    {
        /// All rows have the same keys
        chunks[0].chunk.swap(chunk);
        return chunks;
    }

    /// Scatter the rows into sub-chunks according to selector
    /// ID is already in-place, now collect split columns into each sub-chunk
    for (const auto & col : columns)
    {
        auto split_col = col->scatter(num_chunks, selector);
        assert(split_col.size() == num_chunks);

        /// Move the column to the target chunk columns
        for (size_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index)
            chunks[chunk_index].chunk.addColumn(std::move(split_col[chunk_index]));
    }

    return chunks;
}

}
}
