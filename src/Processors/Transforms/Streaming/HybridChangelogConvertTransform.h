#pragma once

#include <Cluster/Common/SerdeTag.h>
#include <Processors/IProcessor.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>

namespace Poco
{
class Logger;
}

namespace DB::Streaming
{
/// HybridChangelogConvertTransform converts a non-changelog stream to changelog.
/// It is same as ChangelogConvertTransform, but with spill on disk capability
class HybridChangelogConvertTransform final : public IProcessor
{
public:
    HybridChangelogConvertTransform(
        const Block & input_header,
        const Block & output_header,
        std::vector<std::string> key_column_names,
        const std::string & version_column_name,
        std::string spill_dir,
        size_t max_hot_key_count,
        bool backfill_key_unique_);

    ~HybridChangelogConvertTransform() override = default;

    String getName() const override { return "HybridChangelogConvertTransform"; }

    Status prepare() override;
    void work() override;

    bool hasState() const override { return true; }
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    static Block transformOutputHeader(const Block & output_header);

private:
    void createHashTable(const DataTypes & key_column_types, std::string spill_dir, size_t max_hot_key_count);

    void transformEmptyChunk();

    template <typename KeyGetter, typename Map>
    void retractAndIndex(size_t rows, const ColumnRawPtrs & key_columns, Map & map);

    bool backfillingNewKeys() const noexcept { return backfill_key_unique && backfill_started && !backfill_done; }

    RocksPtr getOrCreateRocks();

private:
    std::vector<size_t> key_column_positions;

    /// output_column_positions contains column positions from the input port (upstream)
    /// They are what's need to be projected to output port (downstream)
    /// output_key_column_positions + output_value_column_positions = output_column_positions
    std::vector<size_t> output_column_positions;
    std::vector<size_t> output_key_column_positions;
    std::vector<size_t> output_value_column_positions;
    /// value_column_positions is the Row cached in hybrid hash table
    std::vector<size_t> value_column_positions;

    std::optional<size_t> input_version_column_position;
    std::optional<size_t> version_column_position_in_row;
    std::vector<size_t> key_sizes;
    bool has_nullable_key = false;

    const bool backfill_key_unique = false;
    bool backfill_started = false;
    bool backfill_done = false;

    Chunk output_chunk_header;

    /// std::mutex mutex;

    [[maybe_unused]] SERDE size_t late_rows = 0;

    Port::Data input_data;
    ChunkList output_chunks;

    /// Index blocks by key columns
    HybridHashTableConfig config;
    SERDE HybridHashTableTemplate index;
    SERDE RocksPtr rocks;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    int64_t last_log_ts = 0;
    LoggerPtr logger;
};

}
