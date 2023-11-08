#include <Processors/LightChunkSplitter.h>

#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/WeakHash.h>

namespace DB
{
namespace
{
ALWAYS_INLINE IColumn::Selector hashToSelector(const WeakHash32 & hash, Int16 num_shards)
{
    assert(num_shards > 0 && (num_shards & (num_shards - 1)) == 0);
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        /// Apply intHash64 to mix bits in data.
        /// HashTable internally uses WeakHash32, and we need to get different lower bits not to cause collisions.
        selector[i] = intHash64(data[i]) & (num_shards - 1);
    return selector;
}
}

LightChunkSplitter::LightChunkSplitter(std::vector<size_t> key_column_positions_, UInt16 total_shards_)
    : total_shards(total_shards_), key_column_positions(std::move(key_column_positions_))
{
    assert(total_shards > 0 && (total_shards & (total_shards - 1)) == 0);
}


ShardChunks LightChunkSplitter::split(Chunk & chunk) const
{
    size_t num_rows = chunk.getNumRows();
    assert(num_rows > 0);

    const auto & columns = chunk.getColumns();

    WeakHash32 hash(num_rows);
    for (const auto & key_column_position : key_column_positions)
    {
        assert(columns.size() > key_column_position);
        const auto & key_col = columns[key_column_position]->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
        key_col_no_lc->updateWeakHash32(hash);
    }

    ShardChunks shard_chunks;

    if (num_rows == 1)
    {
        /// Fast path
        shard_chunks.reserve(1);
        UInt16 shard = intHash64(hash.getData()[0]) & (total_shards - 1);
        shard_chunks.emplace_back(shard, std::move(chunk));
        return shard_chunks;
    }

    auto selector = hashToSelector(hash, total_shards);

    std::vector<std::vector<MutableColumnPtr>> sharded_columns;
    sharded_columns.reserve(total_shards);

    for (const auto & column : columns)
        sharded_columns.emplace_back(column->scatter(total_shards, selector));

    shard_chunks.reserve(total_shards);

    auto num_columns = columns.size();
    for (UInt16 shard = 0; shard < total_shards; ++shard)
    {
        /// If sharded column is not null at `shard`
        if (sharded_columns[0][shard])
        {
            Columns chunk_columns;
            chunk_columns.resize(num_columns);

            for (size_t col_pos = 0; col_pos < num_columns; ++col_pos)
                chunk_columns[col_pos] = std::move(sharded_columns[col_pos][shard]);

            auto chunk_rows = chunk_columns[0]->size();
            shard_chunks.emplace_back(shard, Chunk(std::move(chunk_columns), chunk_rows));
        }
    }

    return shard_chunks;
}
}
