#pragma once

#include <Cluster/Common/SerdeTag.h>
#include <Processors/ISimpleTransform.h>
#include <Common/HashMapsTemplate.h>

namespace Poco
{
class Logger;
}

namespace DB
{
namespace Streaming
{
/// VersionsFilterTransform filtering a versioned stream by latest version.
/// It builds a hashtable tracking the key columns / latest version.
class VersionsFilterTransform final : public ISimpleTransform
{
public:
    VersionsFilterTransform(
        const Block & input_header,
        const Block & output_header,
        std::vector<std::string> key_column_names,
        const std::string & version_column_name,
        bool backfill_key_unique_);

    ~VersionsFilterTransform() override = default;

    String getName() const override { return "VersionsFilterTransform"; }

    bool hasState() const override { return true; }
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

    /// \param rows [in/out] Number of rows before and after filtering.
    /// \param columns [in/out] Columns before and after filtering.
    template <typename KeyGetter, typename Map>
    void doFilter(UInt64 & rows, Columns & columns, const ColumnRawPtrs & key_columns, const IColumn & version_column, Map & map);

    bool backfillingNewKeys() const noexcept { return backfill_key_unique && backfill_started && !backfill_done; }

    void transformToOutputColumns(Columns & columns) const;

private:
    std::vector<size_t> output_column_positions;
    std::vector<size_t> key_column_positions;
    size_t version_column_position;
    SerializationPtr version_column_serialization;
    std::vector<size_t> key_sizes;

    const bool backfill_key_unique = false;
    bool backfill_started = false;
    bool backfill_done = false;

    Chunk output_chunk_header;

    SERDE size_t late_rows = 0;
    SERDE HashMapsTemplate<Field> latest_version_map;
    Arena pool;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    int64_t last_log_ts = 0;
    LoggerPtr logger;
};
}
}
