#pragma once

#include <Cluster/Common/SerdeTag.h>
#include <Processors/ISimpleTransform.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>

namespace Poco
{
class Logger;
}

namespace DB
{
namespace Streaming
{
/// HybridVersionsFilterTransform filtering a versioned stream by latest version.
/// It is same as VersionsFilterTransform, but with spill on disk capability
class HybridVersionsFilterTransform final : public ISimpleTransform
{
public:
    HybridVersionsFilterTransform(
        const Block & input_header,
        const Block & output_header,
        std::vector<std::string> key_column_names,
        const std::string & version_column_name,
        bool late_insert_overrides_,
        std::string spill_dir,
        size_t max_hot_key_count,
        const std::string & kv_options,
        bool backfill_key_unique_);

    ~HybridVersionsFilterTransform() override = default;

    String getName() const override { return "HybridVersionsFilterTransform"; }

    bool hasState() const override { return true; }
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

private:
    void
    createHashTable(const DataTypes & key_column_types, std::string spill_dir, size_t max_hot_key_count, const std::string & kv_options);

    void transform(Chunk & chunk) override;

    /// \param rows [in/out] Number of rows before and after filtering.
    /// \param columns [in/out] Columns before and after filtering.
    template <typename KeyGetter, typename Table>
    void doFilter(UInt64 & rows, Columns & columns, const ColumnRawPtrs & key_columns, const IColumn & version_column, Table & table);

    bool backfillingNewKeys() const noexcept { return backfill_key_unique && backfill_started && !backfill_done; }

    void transformToOutputColumns(Columns & columns) const;

    RocksDBPtr getOrCreateRocksDB(const HybridConfig & config);
    RocksDBPtr getOrCreateRocksDB() { return getOrCreateRocksDB(config.base_conf); }

private:
    std::vector<size_t> output_column_positions;
    std::vector<size_t> key_column_positions;
    size_t version_column_position;
    SerializationPtr version_column_serialization;
    std::vector<size_t> key_sizes;
    bool has_nullable_key = false;

    const bool late_insert_overrides = true;

    const bool backfill_key_unique = false;
    bool backfill_started = false;
    bool backfill_done = false;

    Chunk output_chunk_header;

    SERDE size_t late_rows = 0;
    HybridHashTableConfig config;
    SERDE HybridHashTableTemplate latest_version_map;
    SERDE RocksDBPtr rocks;
    FormatSettings format_settings;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    int64_t last_log_ts = 0;
    LoggerPtr logger;
};
}
}
