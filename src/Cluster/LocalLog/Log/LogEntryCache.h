#pragma once

#include <Cluster/Common/EpochSequence.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>
#include <Cluster/Common/Entry.h>

#include <deque>

#include <span>

namespace cluster::nlog
{
struct LogEntryCache
{
    explicit LogEntryCache(size_t max_cached_entries_per_shard_) : max_cached_entries_per_shard(max_cached_entries_per_shard_) { }

    /// \param segment_base_sn The segment base sn
    /// \param start_file_position File position for the first entry in the batch
    void append(
        std::span<const cluster::EntryPtr> batch,
        const std::vector<ByteVector> & batch_data,
        int64_t segment_base_sn,
        uint64_t start_file_position);

    void append(cluster::EntryPtr entry, LogSequenceMetadata current_log_sn_metadata, FetchHint fetch_hint);

    /// Fetch log entries which have sn in range [request_start_sn, request_end_sn) from the cache
    /// \return FetchResult and bool pair which indicates if we need fallback to file system for this fetch
    std::pair<FetchResult, bool> fetch(int64_t request_start_sn, int64_t request_end_sn, size_t max_size, bool memory_log) const;

    /// Trim cached log entries up to target_sn, [target_sn, ...)
    /// \return the new next sequence (FetchHint) after trim if the cache is not empty
    std::optional<FetchHint> trim(int64_t target_sn);

    /// Get epoch sequence for full log range [low_sn, high_sn). If it doesn't have the full range
    /// return empty sequences
    std::vector<EpochSequence> epochSequences(int64_t low_sn, int64_t high_sn) const;

    void clear() noexcept;

    std::optional<int64_t> lastLogSequence() const noexcept;

    const size_t max_cached_entries_per_shard;

    mutable std::mutex mutex;
    uint64_t cached_bytes = 0;

    /// for small object / small medium size container, erase front / push back are pretty efficient
    /// for both std::vector / std::deque. std::deque seems scale better for this tail cache case
    /// https://www.youtube.com/watch?v=LrVi9LHP8Bk

    struct EntryWithSequenceMeta
    {
        LogSequenceMetadata current_log_sn_metadata;
        FetchHint fetch_hint;
        cluster::EntryPtr entry;
    };
    std::deque<EntryWithSequenceMeta> entries;

private:
    inline std::pair<FetchResult, bool> fetchForMemoryLog(int64_t start_sn, int64_t end_sn, size_t max_size) const;
    void clearWithLockHeld() noexcept;
};

}
