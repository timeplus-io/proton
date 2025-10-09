#include <Cluster/Common/Constants.h>
#include <Cluster/LocalLog/Log/LogEntryCache.h>

namespace cluster::nlog
{
void LogEntryCache::append(
    std::span<const cluster::EntryPtr> batch,
    const std::vector<ByteVector> & batch_data,
    int64_t segment_base_sn,
    uint64_t start_file_position)
{
    if (max_cached_entries_per_shard == 0)
        /// fast path if we disabled cache
        return;

    LogSequenceMetadata start_log_sn_metadata;
    start_log_sn_metadata.segment_base_sn = segment_base_sn;

    FetchHint fetch_hint;
    fetch_hint.segment_base_sn = segment_base_sn;

    std::scoped_lock lock{mutex};

    if (batch.size() >= max_cached_entries_per_shard)
        /// TODO, we can do better to slice the batch
        clearWithLockHeld();

    size_t bytes = 0;
    for (size_t i = 0; const auto & entry : batch)
    {
        /// Avoid setting entry epoch for entry cache
        start_log_sn_metadata.entry_sn = entry->sn;
        start_log_sn_metadata.file_position = start_file_position + bytes;

        fetch_hint.sn = entry->sn + 1;
        fetch_hint.file_position = start_file_position + bytes + batch_data[i].size();

        assert(entries.empty() || entries.back().entry->sn + 1 == entry->sn);
        entries.emplace_back(EntryWithSequenceMeta{start_log_sn_metadata, fetch_hint, entry});

        bytes += batch_data[i].size();

        /// For cached bytes, we use approximate size
        cached_bytes += entry->approximateSerializedSize();

        if (entries.size() > max_cached_entries_per_shard)
        {
            cached_bytes -= entries.front().entry->approximateSerializedSize();
            entries.pop_front();
        }

        ++i;
    }
}

void LogEntryCache::append(cluster::EntryPtr entry, LogSequenceMetadata current_log_sn_metadata, FetchHint fetch_hint)
{
    std::scoped_lock lock{mutex};
    cached_bytes += entry->approximateSerializedSize();

    if (entry.use_count() != 1)
        /// Cache an entry copy to prevent other references modified it
        entry = entry->clone();

    assert(entries.empty() || entries.back().entry->sn + 1 == entry->sn);
    entries.emplace_back(EntryWithSequenceMeta{current_log_sn_metadata, fetch_hint, std::move(entry)});

    if (entries.size() > max_cached_entries_per_shard)
    {
        cached_bytes -= entries.front().entry->approximateSerializedSize();
        entries.pop_front();
    }
}

/// [request_start_sn, request_end_sn)
std::pair<FetchResult, bool> LogEntryCache::fetch(int64_t request_start_sn, int64_t request_end_sn, size_t max_size, bool memory_log) const
{
    assert(request_start_sn >= 0);
    assert(request_start_sn < request_end_sn);

    std::scoped_lock lock{mutex};

    if (entries.empty())
        /// No cached log entries, we like to fall through file system to check if there are log entries
        /// Usually reboot case or empty log shard
        return {{}, true};

    if (memory_log)
        return fetchForMemoryLog(request_start_sn, request_end_sn, max_size);

    assert(entries.front().entry);
    auto first_sn{entries.front().entry->sn};
    assert(entries.back().entry);
    auto last_sn{entries.back().entry->sn};

    assert(static_cast<uint64_t>(last_sn - first_sn) <= max_cached_entries_per_shard);

    /// There are several cases
    /// 1. the requested `request_start_sn == LATEST_SN`, we will fallback to the file system logic (Won't happen since caller enforce this)
    /// 2. the requested `request_start_sn == last_sn + 1`, the consumer already catches up the latest log entries in the cache, we will fallback to the file system logic
    /// 3. the requested `request_start_sn > last_sn + 1`, the consumer request something in the future, fallback to the filesystem logic
    /// 4. the requested `request_start_sn == EARLIEST_SN && first_entry->sn == 0`, serve the log entries in the cache (Won't happen since caller enforce this)
    /// 5. the requested `request_start_sn < first_sn`, can't serve this request from cache, fallback to the file system logic
    /// 6. the requested `first_sn <= request_start_sn <= last_sn`, serve this request from cache

    if (request_start_sn > last_sn)
        /// case 1, 2 & 3
        return {{}, true};
    else if (request_start_sn < first_sn)
        /// case 5, out of sequence for cache, fallback to the file system logic
        return {{}, true};

    assert(request_start_sn >= 0 && request_end_sn > request_start_sn);

    /// case 6
    assert(first_sn <= request_start_sn && last_sn >= request_start_sn);

    /// Make sure we don't overflow cache or request more log entries beyond request_end_sn
    request_end_sn = std::min(last_sn + 1, request_end_sn);
    assert(request_end_sn > request_start_sn);

    cluster::EntryPtrs batch_entries;
    batch_entries.reserve(request_end_sn - request_start_sn);

    uint64_t index_start = static_cast<uint64_t>(request_start_sn - first_sn);
    uint64_t index_end = static_cast<uint64_t>(request_end_sn - first_sn);

    assert(0 <= index_start && index_start <= entries.size());
    assert(0 <= index_end && index_end <= entries.size());

    auto it_start = entries.begin() + index_start;
    auto it_end = entries.begin() + index_end;
    auto it = it_start;

    /// std::advance(it, request_start_sn - first_sn);
    size_t total_size = 0;
    for (; it != it_end && total_size < max_size; ++it)
    {
        batch_entries.push_back(it->entry);
        total_size += it->entry->approximateSerializedSize();
    }
    /// >0 means max_size exceeded
    assert(std::distance(it, it_end) >= 0);

    assert(batch_entries.front()->sn == request_start_sn);

    return {
        FetchResult{
            .fetch_hint = std::prev(it)->fetch_hint,
            .fetched_log_sn_metadata = it_start->current_log_sn_metadata,
            .entries = std::move(batch_entries)},
        false};
}

std::pair<FetchResult, bool> LogEntryCache::fetchForMemoryLog(int64_t request_start_sn, int64_t request_end_sn, size_t max_size) const
{
    assert(request_start_sn >= 0);

    auto first_sn{entries.front().entry->sn};
    auto last_sn{entries.back().entry->sn};

    assert(static_cast<uint64_t>(last_sn - first_sn) <= max_cached_entries_per_shard);

    /// There are several cases and we never fallback to file system since it is in-memory log
    /// 1. the requested `request_start_sn == LATEST_SN`, return last_sn + 1 to clients (Won't happen since caller enforces this)
    /// 2. the requested `request_start_sn == last_sn + 1`, return last_sn + 1 to clients
    /// 3. the requested `request_start_sn > last_sn + 1`, the consumer request something in the future, return start_sn
    /// 4. the requested `request_start_sn == EARLIEST_SN`, serve the log entries in range [first_sn, std::min(request_end_sn, last_sn + 1)) from the cache (Won't happen since caller enforces this)
    /// 5. the requested `request_start_sn < first_sn` , serve the log entries in range [first_sn, std::min(request_end_sn, last_sn + 1)) from the cache,
    ///    there are data loss since there are cached log entries which are GCed
    /// 6. the requested `first_sn <= request_start_sn <= last_sn of the cache`, serve this request from cache

    if (request_start_sn == last_sn + 1)
    {
        /// case 1, 2
        return {{.fetch_hint = entries.back().fetch_hint, .fetched_log_sn_metadata = {}, .entries = {}}, false};
    }
    else if (request_start_sn > last_sn + 1)
    {
        /// case 3
        return {{.fetch_hint = FetchHint{.sn = request_start_sn}, .fetched_log_sn_metadata = {}, .entries = {}}, false};
    }
    else if (request_start_sn < first_sn)
    {
        /// case 4, 5
        /// Serve what we have in the cache
        request_start_sn = first_sn;
        request_end_sn = std::min(request_end_sn, last_sn + 1);
    }

    assert(request_start_sn >= 0 && request_end_sn > request_start_sn);

    /// case 6
    assert(first_sn <= request_start_sn && last_sn >= request_start_sn);

    /// Make sure we don't overflow cache or request more log entries beyond request_end_sn
    request_end_sn = std::min(request_end_sn, last_sn + 1);
    assert(request_end_sn > request_start_sn);

    cluster::EntryPtrs batch_entries;
    batch_entries.reserve(request_end_sn - request_start_sn);

    auto it_start = entries.begin() + request_start_sn - first_sn;
    auto it_end = entries.begin() + request_end_sn - first_sn;
    auto it = it_start;

    /// std::advance(it, request_start_sn - first_sn);
    size_t total_size = 0;
    for (; it != it_end && total_size < max_size; ++it)
    {
        batch_entries.push_back(it->entry);
        total_size += it->entry->approximateSerializedSize();
    }

    return {
        FetchResult{
            .fetch_hint = std::prev(it)->fetch_hint,
            .fetched_log_sn_metadata = it_start->current_log_sn_metadata,
            .entries = std::move(batch_entries)},
        false};
}

/// Truncate [target_sn, ...]
std::optional<FetchHint> LogEntryCache::trim(int64_t target_sn)
{
    std::scoped_lock lock{mutex};

    if (entries.empty())
        return {};

    if (entries.back().entry->sn < target_sn)
        return {};

    auto iter_end = entries.end();
    auto iter = std::find_if(
        entries.begin(), iter_end, [target_sn](const auto & entry_with_sn_meta) { return entry_with_sn_meta.entry->sn >= target_sn; });

    /// Reduce cached bytes
    for (auto iter_start = iter; iter_start != iter_end; ++iter_start)
        cached_bytes -= iter_start->entry->approximateSerializedSize();

    entries.erase(iter, iter_end);

    assert(entries.empty() || entries.back().entry->sn == target_sn - 1);

    if (entries.empty())
        return {};

    return entries.back().fetch_hint;
}

/// [low_sn, high_sn)
std::vector<EpochSequence> LogEntryCache::epochSequences(int64_t low_sn, int64_t high_sn) const
{
    assert(low_sn < high_sn);

    if (max_cached_entries_per_shard == 0)
        /// fast path if we disabled cache
        return {};

    std::scoped_lock lock{mutex};
    if (entries.empty())
        return {};

    if (entries.front().entry->sn > low_sn || entries.back().entry->sn + 1 < high_sn)
        return {};

    /// Cache has the full range
    std::vector<EpochSequence> epoch_sequences;
    epoch_sequences.reserve(high_sn - low_sn);

    auto iter = entries.begin() + (low_sn - entries.front().entry->sn);
    assert(iter->entry->sn == low_sn);

    for (; low_sn < high_sn; ++iter, ++low_sn)
        epoch_sequences.emplace_back(1, iter->entry->sn); // epoch

    assert(epoch_sequences.back().sn + 1 == high_sn);

    return epoch_sequences;
}

void LogEntryCache::clear() noexcept
{
    std::scoped_lock lock{mutex};
    clearWithLockHeld();
}

void LogEntryCache::clearWithLockHeld() noexcept
{
    entries.clear();
    cached_bytes = 0;
}

std::optional<int64_t> LogEntryCache::lastLogSequence() const noexcept
{
    std::scoped_lock lock{mutex};

    if (!entries.empty())
        return entries.back().entry->sn;
    return {};
}

}
