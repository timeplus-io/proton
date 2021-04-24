#include "IngestingBlocks.h"

#include <common/logger_useful.h>


namespace DB
{
IngestingBlocks & IngestingBlocks::instance(Int32 timeout_sec)
{
    static IngestingBlocks blocks{timeout_sec};
    return blocks;
}

IngestingBlocks::IngestingBlocks(Int32 timeout_sec_) : timeout_sec(timeout_sec_), log(&Poco::Logger::get("IngestingBlocks"))
{
}

bool IngestingBlocks::add(const String & id, UInt16 block_id)
{
    bool added = false;
    bool hasId = false;
    {
        std::unique_lock guard(rwlock);

        hasId = blockIds.find(id) != blockIds.end();

        auto & blockIdInfo = blockIds[id];
        const auto & res = blockIdInfo.ids.insert(block_id);
        if (res.second)
        {
            blockIdInfo.total++;
            added = true;
        }
    }

    if (!hasId)
    {
        auto now = std::chrono::steady_clock::now();
        /// If there are multiple blocks for an `id`
        /// only stamp the first block
        std::lock_guard guard(lock);
        timedBlockIds.emplace_back(now, id);
    }

    removeExpiredBlockIds();

    return added;
}

inline void IngestingBlocks::removeExpiredBlockIds()
{
    /// Remove expired block ids
    std::vector<std::pair<String, size_t>> expired;
    {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard guard(lock);

        while (!timedBlockIds.empty())
        {
            const auto & p = timedBlockIds.front();

            if (std::chrono::duration_cast<std::chrono::seconds>(now - p.first).count() >= timeout_sec)
            {
                expired.push_back({});
                expired.back().first = p.second;

                /// Remove from blockIds
                std::unique_lock rwguard(rwlock);
                auto iter = blockIds.find(p.second);
                assert(iter != blockIds.end());

                expired.back().second = iter->second.ids.size();
                blockIds.erase(iter);

                /// Remove from timedBlockIds
                timedBlockIds.pop_front();
            }
            else
            {
                break;
            }
        }
    }

    for (const auto & p : expired)
    {
        if (p.second > 0)
        {
            LOG_WARNING(log, "Timed out and there are {} pending data blocks waiting to be committed for query_id={}", p.second, p.first);
        }
        else
        {
            LOG_TRACE(log, "Timed out and the data blocks associated with query_id={} have been successfully ingested", p.first);
        }
    }
}

bool IngestingBlocks::remove(const String & id, UInt16 block_id)
{
    LOG_TRACE(log, "Removing query_id={} block_id={}", id, block_id);

    std::unique_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        auto iterId = iter->second.ids.find(block_id);
        if (iterId != iter->second.ids.end())
        {
            iter->second.ids.erase(iterId);
            return true;
        }
    }
    return false;
}

std::pair<String, Int32> IngestingBlocks::status(const String & id) const
{
    std::shared_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        Int32 progress = (iter->second.total - iter->second.ids.size()) * 100 / iter->second.total;

        if (iter->second.err != 0)
            return std::make_pair("Failed", progress);

        if (progress < 100)
            return std::make_pair("Processing", progress);
        else
            return std::make_pair("Succeeded", progress);
    }
    return std::make_pair("Unknown", -1);
}

void IngestingBlocks::getStatuses(const std::vector<String> & poll_ids, std::vector<IngestingBlocks::IngestStatus> & statuses) const
{
    std::shared_lock guard(rwlock);
    for (const auto & id : poll_ids)
    {
        auto iter = blockIds.find(id);
        if (iter == blockIds.end())
        {
            statuses.push_back({id, "Unknown", -1});
            continue;
        }

        Int32 progress = (iter->second.total - iter->second.ids.size()) * 100 / iter->second.total;

        if (iter->second.err != 0)
            statuses.push_back({id, "Failed", progress});

        if (progress < 100)
            statuses.push_back({id, "Processing", progress});
        else
            statuses.push_back({id, "Succeeded", progress});
    }
}

size_t IngestingBlocks::outstandingBlocks() const
{
    size_t total = 0;
    std::shared_lock guard(rwlock);
    for (const auto & p : blockIds)
    {
        total += p.second.ids.size();
    }
    return total;
}

void IngestingBlocks::fail(const String & id, UInt16 err)
{
    std::unique_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        iter->second.err = err;
    }
}
}
