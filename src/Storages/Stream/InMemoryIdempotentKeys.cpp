#include <Storages/Stream/InMemoryIdempotentKeys.h>

#include <Cluster/Common/Constants.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <ranges>
#include <fmt/ranges.h>

namespace DB
{
InMemoryIdempotentKeys::InMemoryIdempotentKeys(size_t max_ids_, const std::deque<IdempotentKey> & idem_keys_, LoggerPtr logger_)
    : max_ids(max_ids_), keys(idem_keys_), logger(logger_)
{
    for (const auto & [_, key] : keys)
        keys_index.emplace(key);
}

bool InMemoryIdempotentKeys::add(Int64 sn, String & idem_key, const char * deduped_log_prefix)
{
    if (idem_key.empty())
        return true; /// Treats it as always new

    {
        std::shared_lock lock(mutex);
        if (keys_index.contains(idem_key))
        {
            if (deduped_log_prefix)
                LOG_INFO(logger, "{}: idempotent_key={} sn={}", deduped_log_prefix, idem_key, sn);
            return false;
        }
    }

    std::unique_lock lock(mutex);

    if (keys_index.contains(idem_key))
    {
        if (deduped_log_prefix)
            LOG_INFO(logger, "{}: idempotent_key={} sn={}", deduped_log_prefix, idem_key, sn);
        return false;
    }

    while (keys.size() >= max_ids)
    {
        [[maybe_unused]] auto removed = keys_index.erase(keys.front().second);
        chassert(removed == 1);

        keys.pop_front();
    }

    keys.emplace_back(sn, std::move(idem_key));

    [[maybe_unused]] auto [_, inserted] = keys_index.emplace(keys.back().second);
    chassert(inserted);

    chassert(keys.size() == keys_index.size());

    has_updates_since_last_serialization = true;
    return true;
}

size_t InMemoryIdempotentKeys::rewindTo(Int64 max_sn)
{
    std::unique_lock lock(mutex);

    size_t dropped = 0;
    while (!keys.empty() && keys.back().first > max_sn)
    {
        [[maybe_unused]] auto removed = keys_index.erase(keys.back().second);
        chassert(removed == 1);
        keys.pop_back();
        ++dropped;
    }

    chassert(keys.size() == keys_index.size());

    if (dropped > 0)
        has_updates_since_last_serialization = true;

    return dropped;
}

size_t InMemoryIdempotentKeys::size() const
{
    std::shared_lock lock(mutex);
    return keys.size();
}

bool InMemoryIdempotentKeys::hasUpdatesSinceLastSerialization() const
{
    std::shared_lock lock(mutex);
    return has_updates_since_last_serialization;
}

size_t InMemoryIdempotentKeys::approximateSerializedSize() const noexcept
{
    std::shared_lock lock(mutex);
    return sizeof(max_ids) + sizeof(keys.size()) + keys.size() * 16; /// 16 is a rough estimate of the size of a string
}

void InMemoryIdempotentKeys::serialize(DB::WriteBuffer & wb, uint16_t) const
{
    std::shared_lock lock(mutex);

    DB::writeVarUInt(max_ids, wb);
    DB::writeVarUInt(keys.size(), wb);
    for (const auto & elem : keys)
        DB::writeBinary(elem, wb);

    has_updates_since_last_serialization = false;
}

void InMemoryIdempotentKeys::deserialize(DB::ReadBuffer & rb, uint16_t)
{
    std::unique_lock lock(mutex);

    DB::readVarUInt(max_ids, rb);

    keys.clear();
    keys_index.clear();

    size_t size = 0;
    DB::readVarUInt(size, rb);
    keys.resize(size);
    for (auto & elem : keys)
    {
        DB::readBinary(elem, rb);
        keys_index.emplace(elem.second);
    }
}

std::string InMemoryIdempotentKeys::string() const
{
    std::shared_lock lock(mutex);
    return fmt::format(
        "max_ids={} keys_num={} keys={}",
        max_ids,
        keys.size(),
        fmt::join(keys | std::views::transform([](const auto & elem) { return fmt::format("{}-{}", elem.first, elem.second); }), ","));
}

InMemoryIdempotentKeysPtr InMemoryIdempotentKeys::snapshotTo(Int64 max_sn, size_t max_ids_) const
{
    auto new_idem_keys = std::make_unique<InMemoryIdempotentKeys>(max_ids_, logger);
    if (max_sn >= cluster::Constants::LogStartSN)
    {
        std::shared_lock lock(mutex);
        for (const auto & [sn, key] : keys)
        {
            if (sn > max_sn)
                break;

            std::string copy_key{key};
            new_idem_keys->add(sn, copy_key);
        }
    }
    return new_idem_keys;
}

}
