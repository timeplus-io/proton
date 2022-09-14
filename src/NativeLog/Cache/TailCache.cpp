#include "TailCache.h"

#include <Common/Stream.h>

namespace nlog
{
TailCache::TailCache(const TailCacheConfig & config_) : config(config_)
{
}

void TailCache::put(const StreamShard & stream_shard, RecordPtr record)
{
    CacheEntryPtr entry;
    {
        std::scoped_lock lock(mutex);
        auto it = records.find(stream_shard);
        if (it != records.end())
        {
            entry = it->second;
        }
        else
        {
            /// stream not exists, insert stream
            auto res{records.emplace(stream_shard, std::make_shared<CacheEntry>())};
            assert(res.second);
            entry = res.first->second;
        }
    }

    size += record->totalSerializedBytes();
    entries += 1;
    {
        assert(entry);
        std::unique_lock lock(entry->mutex);
        entry->shard_records.push_back(std::move(record));
        if (entry->shard_records.size() > static_cast<size_t>(config.max_cached_entries_per_shard))
        {
            size -= entry->shard_records.front()->totalSerializedBytes();
            entries -= 1;

            entry->shard_records.erase(entry->shard_records.begin());
        }
    }

    entry->new_records_cv.notify_all();
}

std::pair<RecordPtrs, bool> TailCache::get(const StreamShard & stream_shard, int64_t wait_ms, int64_t start_sn, bool from_cache_only) const
{
    assert(wait_ms > 0);

    if (start_sn == EARLIEST_SN && !from_cache_only)
        /// We can't serve earliest sn from cache since we don't know the earliest sn in file system
        /// fallback to log
        return {{}, true};

    CacheEntryPtr entry;
    {
        std::scoped_lock lock(mutex);
        auto it = records.find(stream_shard);
        if (it != records.end())
            entry = it->second;
    }

    if (!entry)
        /// Didn't find stream_shard in cache, fallback to log
        return {{}, true};

    {
        std::unique_lock lock(entry->mutex);

        /// We still have a chance when TailCache::put insert a new entry for a stream_shard, but before
        /// inserting the records to the entry, a TailCache::get chimes in
        if (entry->shard_records.empty())
            /// We like client to spin / retry since this is a very short spin and the next time, the cache is probably not empty
            return {{}, false};

        auto first_sn{entry->shard_records.front()->getSN()};
        auto last_sn{entry->shard_records.back()->getSN()};

        assert(last_sn - first_sn <= config.max_cached_entries_per_shard);

        /// There are several cases
        /// 0. the requested `start_sn == LATEST_SN`, we will wait for future records beyond last_sn
        /// 1. the requested `start_sn < first_sn of the cache` , can't serve this request from cache (consumer will catch up from file system)
        /// 2. the requested `first_sn <= start_sn <= last_sn of the cache`, serve this request from cache
        /// 3. the requested `start_sn == last_sn + 1`, the consumer already catches up the latest records in the cache, we will wait for future records
        /// 4. the requested `start_sn > last_sn + 1`, the consumer request something in the future which shall not happen, we can have 2 different handling
        ///    logic for this request: a. throw an Exception telling consumer the request is out of sequence or b. wait for future sn to be cached. We pick b. here
        /// 5. the requested `start_sn == EARLIEST_SN && from_cache_only=true`, serve the records in the cache

        /// case 5
        if (start_sn == EARLIEST_SN)
            /// service what we have in the cache
            start_sn = first_sn;

        /// case 0
        if (start_sn == LATEST_SN)
            start_sn = last_sn + 1;

        if (start_sn >= last_sn + 1)
        {
            /// case 3, 4
            auto pred = [entry, start_sn] { return entry->shard_records.back()->getSN() >= start_sn; };
            if (!entry->new_records_cv.wait_for(lock, std::chrono::milliseconds(wait_ms), pred))
                /// Timed out and no new records has been inserted to cache
                return {{}, false};

            /// fall-through, we have waited some new records within time out
            /// we need re-evaluate first_sn, last_sn since during waiting new records has been ingested, old records may be evicted
            first_sn = entry->shard_records.front()->getSN();
            last_sn = entry->shard_records.back()->getSN();
        }

        /// case 1
        if (start_sn < first_sn)
        {
            if (from_cache_only)
                /// There may be data loss
                start_sn = first_sn;
            else
                /// Out of sequence for cache, fallback to log
                return {{}, true};
        }

        /// case 2
        if (first_sn <= start_sn && last_sn >= start_sn)
        {
            RecordPtrs results;
            results.reserve(last_sn - start_sn + 1);
            auto it = entry->shard_records.begin();
            std::advance(it, start_sn - first_sn);
            for (auto end_it = entry->shard_records.end(); it != end_it; ++it)
                results.push_back(*it);

            return {std::move(results), false};
        }
    }

    __builtin_unreachable();
}

void TailCache::remove(const StreamShard & stream_shard)
{
    CacheEntryPtr entry;
    {
        std::scoped_lock lock(mutex);
        auto it = records.find(stream_shard);
        if (it != records.end())
        {
            entry = it->second;
            records.erase(it);
        }
    }

    if (entry)
        entry->new_records_cv.notify_all();
}

}
