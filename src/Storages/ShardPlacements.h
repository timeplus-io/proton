#pragma once

#include <Common/SharedMutex.h>

#include <ranges>
#include <vector>

namespace DB
{
/// Assume T is a pointer like type
template <typename T>
struct ShardPlacements
{
    void setShards(std::vector<T> shards_) noexcept
    {
        std::unique_lock wlock{rw_mu};
        shards.swap(shards_);
    }

    std::vector<T> getShards() const
    {
        std::shared_lock rlock{rw_mu};
        return shards;
    }

    T getShard(uint32_t shard_id) const noexcept
    {
        std::shared_lock rlock{rw_mu};
        if (shard_id < shards.size() && shards[shard_id] && shards[shard_id]->shard() == shard_id)
            return shards[shard_id];

        // Fallback to linear search
        for (const auto & shard : shards)
        {
            if (shard && shard->shard() == shard_id)
                return shard;
        }

        return {};
    }

    T operator[](size_t index) const
    {
        std::shared_lock rlock{rw_mu};
        return shards[index];
    }

    T front() const
    {
        std::shared_lock rlock{rw_mu};
        return shards.front();
    }

    T back() const
    {
        std::shared_lock rlock{rw_mu};
        return shards.back();
    }

    bool empty() const noexcept
    {
        std::shared_lock rlock{rw_mu};
        return shards.empty();
    }

    size_t size() const noexcept
    {
        std::shared_lock rlock{rw_mu};
        return shards.size();
    }

public:
    mutable SharedMutex rw_mu;

    /// Ascending order by shard id
    std::vector<T> shards;
};

}
