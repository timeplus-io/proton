#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/MetaStore/MetaDB.h>

#include <Common/Logger.h>

namespace cluster::meta
{
/// LocalMetaQueue implements a persist-first in-memory queue for metadata requests
/// Ensures durability by persisting to MetaDB before
/// queuing in memory, supports contiguous fetch operations, and provides backpressure.
class LocalMetaQueue
{
public:
    static constexpr size_t DEFAULT_MAX_ENTRIES = 10000;
    static constexpr size_t DEFAULT_MAX_BYTES = 100 * 1024 * 1024; // 100MB

    LocalMetaQueue(
        std::shared_ptr<MetaDB> meta_db_,
        LoggerPtr logger_,
        size_t max_entries = DEFAULT_MAX_ENTRIES,
        size_t max_bytes = DEFAULT_MAX_BYTES);

    ~LocalMetaQueue();

    /// Append a new metadata request
    /// 1. Persists to MetaDB with sync write
    /// 2. Adds to in-memory queue
    /// 3. Returns the assigned sequence number
    /// Blocks if queue is at capacity (backpressure)
    CallResultV<uint64_t> append(const std::string & serialized_data, uint64_t correlation_id = 0);

    /// Fetch metadata requests starting from sequence number
    /// Returns contiguous entries up to max_bytes
    /// Supports timeout for blocking fetch
    CallResultV<std::shared_ptr<nlog::FetchResult>> fetch(int64_t from_sn, uint64_t max_bytes, int64_t max_wait_ms) const;

    /// Mark entries as consumed for single-consumer semantics
    /// Erases entries up to and including the given sequence number
    void eraseUpTo(uint64_t sn);

    /// Get the next sequence number that will be assigned
    uint64_t nextSequenceNumber() const;

    /// Get the current queue size (for monitoring)
    size_t size() const;

    /// Get the current queue bytes (for monitoring)
    size_t bytes() const;

    /// Initialize from persistent state during recovery
    /// Loads pending requests from MetaDB starting after last_applied_sn
    Error recoverFromPersistent(uint64_t last_applied_sn);

    /// Shutdown the queue (stop accepting new appends)
    void shutdown();

private:
    struct QueueEntry
    {
        uint64_t sequence_number;
        uint64_t correlation_id = 0;
        std::string data;

        size_t sizeBytes() const { return sizeof(sequence_number) + sizeof(correlation_id) + data.size(); }
    };

    mutable std::mutex mutex;
    mutable std::condition_variable not_empty_cv;
    std::condition_variable not_full_cv;

    std::deque<QueueEntry> queue;
    std::atomic<uint64_t> next_sn{1};
    std::atomic<size_t> current_bytes{0};
    std::atomic<bool> shutdown_flag{false};

    const size_t max_entries;
    const size_t max_bytes;

    std::shared_ptr<MetaDB> meta_db;
    LoggerPtr logger;

    /// Helper to check if queue is at capacity
    bool isAtCapacity() const;

    /// Wait for space with timeout
    bool waitForSpace(std::unique_lock<std::mutex> & lock, std::chrono::milliseconds timeout);
};

using LocalMetaQueuePtr = std::shared_ptr<LocalMetaQueue>;

}
