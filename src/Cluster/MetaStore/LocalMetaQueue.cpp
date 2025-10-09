#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/Requests/RequestHeader.h>

#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int SYSTEM_ERROR;
}

namespace cluster::meta
{

LocalMetaQueue::LocalMetaQueue(std::shared_ptr<MetaDB> meta_db_, LoggerPtr logger_, size_t max_entries_, size_t max_bytes_)
    : max_entries(max_entries_), max_bytes(max_bytes_), meta_db(std::move(meta_db_)), logger(logger_)
{
    LOG_DEBUG(logger, "LocalMetaQueue initialized with max_entries={}, max_bytes={}", max_entries, max_bytes);
}

LocalMetaQueue::~LocalMetaQueue()
{
    shutdown();
}

CallResultV<uint64_t> LocalMetaQueue::append(const std::string & serialized_data, uint64_t correlation_id)
{
    if (shutdown_flag.load())
        return CallResultV<uint64_t>{DB::ErrorCodes::SYSTEM_ERROR, "LocalMetaQueue is shut down"};

    std::unique_lock lock(mutex);

    /// Wait for space if at capacity (backpressure)
    if (isAtCapacity())
    {
        LOG_DEBUG(logger, "Queue at capacity, waiting for space");
        if (!waitForSpace(lock, std::chrono::seconds(30)))
        {
            return CallResultV<uint64_t>{DB::ErrorCodes::TIMEOUT_EXCEEDED, "Timeout waiting for queue space"};
        }
    }

    /// Double-check shutdown after wait
    if (shutdown_flag.load())
        return CallResultV<uint64_t>{DB::ErrorCodes::SYSTEM_ERROR, "LocalMetaQueue is shut down"};

    uint64_t sn = next_sn.fetch_add(1);

    /// Persist to MetaDB first (sync write)
    auto persist_result = meta_db->savePendingRequest(sn, serialized_data);
    if (persist_result.hasError())
    {
        /// Roll back sequence number on failure
        next_sn.fetch_sub(1);
        return CallResultV<uint64_t>{std::move(persist_result)};
    }
    
    /// Add to in-memory queue with the provided correlation_id
    QueueEntry entry{.sequence_number = sn, .correlation_id = correlation_id, .data = serialized_data};
    size_t entry_size = entry.sizeBytes();
    queue.push_back(std::move(entry));

    current_bytes.fetch_add(entry_size);

    LOG_DEBUG(logger, "Appended request sn={}, correlation_id={}, size={}, queue_size={}", sn, correlation_id, serialized_data.size(), queue.size());

    not_empty_cv.notify_all();

    return CallResultV<uint64_t>{sn};
}

CallResultV<std::shared_ptr<nlog::FetchResult>> LocalMetaQueue::fetch(int64_t from_sn, uint64_t max_bytes_param, int64_t max_wait_ms) const
{
    std::unique_lock lock(mutex);

    /// Check shutdown flag first, before any other logic
    if (shutdown_flag.load())
    {
        /// Return empty result on shutdown
        auto result = std::make_shared<nlog::FetchResult>();
        result->log_start_sn = from_sn;
        result->log_committed_sn = from_sn;
        return CallResultV{std::move(result)};
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(max_wait_ms);

    /// Wait for data if queue is empty or doesn't have the requested sequence
    while (queue.empty() || queue.front().sequence_number > static_cast<uint64_t>(from_sn))
    {
        if (shutdown_flag.load())
        {
            /// Return empty result on shutdown
            auto result = std::make_shared<nlog::FetchResult>();
            result->log_start_sn = from_sn;
            result->log_committed_sn = from_sn;
            return CallResultV{std::move(result)};
        }

        if (max_wait_ms == 0)
        {
            /// Non-blocking fetch - return empty result
            auto result = std::make_shared<nlog::FetchResult>();
            result->log_start_sn = from_sn;
            result->log_committed_sn = from_sn;
            return CallResultV{std::move(result)};
        }

        if (not_empty_cv.wait_until(lock, deadline) == std::cv_status::timeout)
        {
            /// Timeout - return empty result
            auto result = std::make_shared<nlog::FetchResult>();
            result->log_start_sn = from_sn;
            result->log_committed_sn = from_sn;
            return CallResultV{std::move(result)};
        }
    }

    auto result = std::make_shared<nlog::FetchResult>();
    result->log_start_sn = from_sn;
    result->log_committed_sn = from_sn;

    size_t bytes_fetched = 0;

    /// Find starting position for contiguous fetch
    auto it = queue.begin();
    while (it != queue.end() && it->sequence_number < static_cast<uint64_t>(from_sn))
    {
        ++it;
    }

    /// Convert pending entries to metadata entries for fetch
    int64_t expected_sn = from_sn;
    while (it != queue.end() && bytes_fetched + it->data.size() <= max_bytes_param)
    {
        /// Check for sequence continuity
        if (it->sequence_number != static_cast<uint64_t>(expected_sn))
        {
            LOG_WARNING(logger, "Sequence gap detected: expected={}, actual={}", expected_sn, it->sequence_number);
            break;
        }
        
        auto entry = std::make_shared<cluster::Entry>();
        entry->sn = static_cast<int64_t>(it->sequence_number);
        entry->correlation_id = it->correlation_id;
        entry->data = cluster::ByteVector(it->data);
        
        result->entries.push_back(entry);

        bytes_fetched += it->data.size();
        expected_sn++;
        ++it;
    }

    /// Update fetch hint for next fetch
    if (!result->entries.empty())
    {
        /// fetch_hint is only used for distributed fetching
        result->fetched_log_sn_metadata.entry_sn = result->entries.front()->sn;
        result->log_committed_sn = result->entries.back()->sn;
    }

    LOG_DEBUG(logger, "Fetched {} entries, {} bytes, next_sn={}", result->entries.size(), bytes_fetched, expected_sn);

    return CallResultV{std::move(result)};
}

void LocalMetaQueue::eraseUpTo(uint64_t sn)
{
    std::unique_lock lock(mutex);

    size_t erased_count = 0;
    size_t erased_bytes = 0;
    size_t initial_queue_size = queue.size();
    size_t initial_bytes = current_bytes.load();

    while (!queue.empty() && queue.front().sequence_number <= sn)
    {
        erased_bytes += queue.front().sizeBytes();
        queue.pop_front();
        erased_count++;
    }

    if (erased_count > 0)
    {
        current_bytes.fetch_sub(erased_bytes);
        LOG_INFO(logger, "Erased {} entries up to sn={}, freed {} bytes, queue size: {} -> {}, bytes: {} -> {}", 
                 erased_count, sn, erased_bytes, initial_queue_size, queue.size(), initial_bytes, current_bytes.load());

        /// Notify waiters that space is available
        not_full_cv.notify_all();
    }
    else
    {
        LOG_INFO(logger, "No entries to erase up to sn={}, queue size={}, front_sn={}", 
                 sn, queue.size(), queue.empty() ? 0 : queue.front().sequence_number);
    }
}

uint64_t LocalMetaQueue::nextSequenceNumber() const
{
    return next_sn.load();
}

size_t LocalMetaQueue::size() const
{
    std::lock_guard lock(mutex);
    return queue.size();
}

size_t LocalMetaQueue::bytes() const
{
    return current_bytes.load();
}

Error LocalMetaQueue::recoverFromPersistent(uint64_t last_applied_sn)
{
    LOG_INFO(logger, "Recovering LocalMetaQueue from persistent state after sn={}", last_applied_sn);

    auto pending_result = meta_db->iteratePendingRequestsAfter(last_applied_sn);
    if (pending_result.hasError())
    {
        LOG_ERROR(logger, "Failed to iterate pending requests: {}", pending_result.errorString());
        return pending_result.err;
    }

    std::lock_guard lock(mutex);

    /// Clear any existing entries
    queue.clear();
    current_bytes = 0;

    /// Load pending requests into queue
    size_t loaded_count = 0;
    size_t loaded_bytes = 0;
    uint64_t max_sn = last_applied_sn;

    for (const auto & [sn, data] : pending_result.result)
    {
        /// After restart, all client connections are gone.
        /// Use correlation_id=0 to indicate recovered entries that should be applied
        /// but won't send responses (clients are disconnected anyway)
        QueueEntry entry{.sequence_number = sn, .correlation_id = 0, .data = data};
        loaded_bytes += entry.sizeBytes();
        queue.push_back(std::move(entry));

        loaded_count++;
        max_sn = std::max(max_sn, sn);
    }

    current_bytes = loaded_bytes;

    /// Set next sequence number to one past the highest recovered
    next_sn = max_sn + 1;

    LOG_INFO(logger, "Recovered {} pending requests, {} bytes, next_sn={}", loaded_count, loaded_bytes, next_sn.load());

    /// Notify any waiters
    not_empty_cv.notify_all();

    return Error();
}

void LocalMetaQueue::shutdown()
{
    {
        std::lock_guard lock(mutex);
        shutdown_flag = true;
    }

    /// Wake up all waiters
    not_empty_cv.notify_all();
    not_full_cv.notify_all();

    LOG_DEBUG(logger, "LocalMetaQueue shut down");
}

bool LocalMetaQueue::isAtCapacity() const
{
    return queue.size() >= max_entries || current_bytes.load() >= max_bytes;
}

bool LocalMetaQueue::waitForSpace(std::unique_lock<std::mutex> & lock, std::chrono::milliseconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (isAtCapacity() && !shutdown_flag.load())
    {
        if (not_full_cv.wait_until(lock, deadline) == std::cv_status::timeout)
        {
            return false;
        }
    }

    return !shutdown_flag.load();
}

}
