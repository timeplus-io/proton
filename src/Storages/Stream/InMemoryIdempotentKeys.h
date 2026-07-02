#pragma once

#include <base/types.h>
#include <Common/SharedMutex.h>

#include <absl/container/flat_hash_set.h>
#include <boost/noncopyable.hpp>

#include <deque>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{
class WriteBuffer;
class ReadBuffer;

using IdempotentKey = std::pair<Int64 /*sn*/, String /*key*/>;

class InMemoryIdempotentKeys;
using InMemoryIdempotentKeysPtr = std::unique_ptr<InMemoryIdempotentKeys>;

class InMemoryIdempotentKeys final : private boost::noncopyable
{
public:
    InMemoryIdempotentKeys(size_t max_ids_, LoggerPtr logger_) : max_ids(max_ids_), logger(logger_) { }
    InMemoryIdempotentKeys(size_t max_ids_, const std::deque<IdempotentKey> & keys_, LoggerPtr logger_);

    /// Creates a clone of the current object, including only the keys with sequence numbers [..., max_sn],
    /// in other words, not including, the specified (max_sn, ...].
    InMemoryIdempotentKeysPtr snapshotTo(Int64 max_sn, size_t max_ids_) const;

    /// \return true if the key is new; false if it was already present.
    bool add(Int64 sn, String & idem_key, const char * deduped_log_prefix = nullptr);

    const IdempotentKey & lastKey() const noexcept { return keys.back(); }

    /// Drops entries with sn > \p max_sn. Assumes sn-ascending order. \return dropped count.
    size_t rewindTo(Int64 max_sn);

    void setMaxIDs(size_t max_ids_) { max_ids = max_ids_; }

    size_t size() const;

    bool hasUpdatesSinceLastSerialization() const;

    size_t approximateSerializedSize() const noexcept;
    void serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t /*version*/);
    std::string string() const;

private:
    size_t max_ids = 1000;

    mutable DB::SharedMutex mutex;
    std::deque<IdempotentKey> keys;
    absl::flat_hash_set<std::string_view> keys_index;
    mutable bool has_updates_since_last_serialization = false;

    LoggerPtr logger;
};

}
