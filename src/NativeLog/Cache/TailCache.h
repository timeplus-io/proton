#pragma once

#include "TailCacheConfig.h"

#include <Record/Record.h>
#include <Common/StreamShard.h>

#include <memory>

#include <absl/container/flat_hash_map.h>
#include <boost/noncopyable.hpp>

namespace nlog
{
class TailCache final : private boost::noncopyable
{
public:
    explicit TailCache(const TailCacheConfig & config_);

    void put(const StreamShard & stream_shard, RecordPtr record);

    std::pair<RecordPtrs, bool> get(const StreamShard & stream_shard, int64_t wait_ms, int64_t start_sn, bool from_cache_only = false) const;

    void remove(const StreamShard & stream_shard);

private:
    TailCacheConfig config;

    struct CacheEntry
    {
        mutable std::mutex mutex;
        std::condition_variable new_records_cv;

        uint64_t size = 0;
        std::vector<RecordPtr> shard_records;
    };

    using CacheEntryPtr = std::shared_ptr<CacheEntry>;
    /// using ShardCacheEntries = std::unordered_map<int32_t, CacheEntryPtr>;

    std::atomic_uint_fast64_t size = 0;
    std::atomic_uint_fast64_t entries = 0;

    mutable std::mutex mutex;
    absl::flat_hash_map<StreamShard, CacheEntryPtr> records;
};

using TailCachePtr = std::shared_ptr<TailCache>;
}
