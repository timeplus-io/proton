#pragma once

#include <Processors/IProcessor.h>

#include <Common/HashMapsTemplate.h>
#include <Interpreters/Streaming/RowRefs.h>

namespace Poco
{
class Logger;
}

namespace DB
{
namespace Streaming
{
/// ChangelogConvertTransform converts a non-changelog stream to changelog.
/// It builds a hashtable tracking the key columns / value changes and emit changelog
/// rows to down stream. It assumes the key columns are unique for data sources. For now, it is used to
/// tracking the changes for versioned-kv stream in real-time.
/// For example, assume stream `kv` has primary key `k`, here is one example for emit process
/// Initially, input is empty. Please note input header always has primary key column `k` and `version` column
/// k, v, s, version
/// output is also empty. Please note output header may be not having primary key column nor version column, but it adds `_tp_delta` column
/// v, s, _tp_delta

/// When row `k1` gets streamed in from input, we insert it to `blocks` and build index in hash table for it
///  k,    v,   s, version
/// 'k1', 'v1', 7, 1 # k1 is inserted
/// and we emit
///  v,   s, _tp_delta
/// 'v1, 7, 1
/// When row `k2` gets streamed in from input, we also insert it to `blocks` and build index in hash table for it
///  k,    v,   s, version
/// 'k1', 'v1', 7, 1
/// 'k2', 'v2', 9, 1 # k2 is inserted
/// and we emit
///  v,   s, _tp_delta
/// 'v2', 9, 1
/// When there is another row `'k1', 'v11', 11, 2` which acts like an update to `k1`, it first update the `blocks` and index
///  k,    v,   s, version
/// 'k2', 'v2', 9, 1
/// 'k1', 'v11',11, 2 # k1 is updated
/// and we emit
///  v,   s, _tp_delta
/// 'v1', 7, -1
/// 'v1', 11, 1
///
/// Late event handling: we assume version column is monotonically increased. Whenever there is a late row which has smaller value in version column,
/// we choose dropping the row on the floor.

class ChangelogConvertTransform final : public IProcessor
{
public:
    ChangelogConvertTransform(const Block & input_header, const Block & output_header, std::vector<std::string> key_column_names, const std::string & version_column_name);

    ~ChangelogConvertTransform() override = default;

    String getName() const override { return "ChangelogConvertTransform"; }

    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformOutputHeader(const Block & output_header);

private:
    void transformEmptyChunk();

    template <typename KeyGetter, typename Map>
    void retractAndIndex(size_t rows, const ColumnRawPtrs & key_columns, Map & map);

private:
    std::vector<size_t> output_column_positions;
    std::vector<size_t> key_column_positions;
    std::optional<size_t> version_column_position;
    std::vector<size_t> key_sizes;

    Chunk output_chunk_header;

    /// std::mutex mutex;

    SERDE size_t late_rows = 0;
    SERDE CachedBlockMetrics cached_block_metrics;
    SERDE RefCountDataBlockList<LightChunk> source_chunks;

    Port::Data input_data;
    ChunkList output_chunks;

    /// Index blocks by key columns
    SERDE HashMapsTemplate<std::unique_ptr<RowRefWithRefCount<LightChunk>>> index;
    Arena pool;

    int64_t last_log_ts = 0;
    Poco::Logger * logger;
};
}
}
