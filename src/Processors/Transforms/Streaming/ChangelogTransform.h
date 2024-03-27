#pragma once

#include <Processors/IProcessor.h>

/// #include <Interpreters/Streaming/RowRefs.h>

/// #include <absl/container/flat_hash_map.h>

namespace DB
{
namespace Streaming
{
/// Changelog's input is a changelog. It does
/// 1. Compact input chunk if it has multiple appearance of the same key
/// 2. Handle out of order changelog as much as possible
/// 3. Handle duplicate changelog as much as possible
/// 4. Split final chunk into retract chunk and update chunk
/// For now, we have only implemented the split and assuming the changelog stream is totally ordered and exactly once
class ChangelogTransform final : public IProcessor
{
public:
    ChangelogTransform(const Block & input_header, const Block & output_header, std::vector<std::string> key_column_names, const std::string & version_column_name);

    ~ChangelogTransform() override = default;

    String getName() const override { return "ChangelogTransform"; }

    Status prepare() override;
    void work() override;

private:
    void transformChunk(Chunk & chunk);

private:
    size_t delta_column_position;
    std::vector<size_t> output_column_positions;
    std::vector<size_t> key_column_positions;
    std::optional<size_t> version_column_position;

    Chunk output_chunk_header;

    /// size_t late_rows = 0;
    /// CachedBlockMetrics metrics;
    /// RefCountBlockList<Chunk> source_chunks;

    /// Index blocks by key columns
    /// using HashMap = absl::flat_hash_map<UInt128, RowRefWithRefCount<Chunk>, UInt128TrivialHash>;
    /// HashMap index;

    Port::Data input_data;
    ChunkList output_chunks;

    /// int64_t last_log_ts = 0;
    /// Poco::Logger * logger;
};
}
}
