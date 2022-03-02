#pragma once

#include <Core/Types.h>
#include <Poco/Logger.h>

#include <boost/core/noncopyable.hpp>

#include <chrono>
#include <deque>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>


namespace DB
{
class IngestingBlocks final : public boost::noncopyable
{
public:
    static IngestingBlocks & instance(Int32 timeout_sec = 120);

    explicit IngestingBlocks(Int32 timeout_sec = 120);
    ~IngestingBlocks() = default;

    /// add `block_id` of query `id`. One query `id` can have several `blocks`
    /// return true if add successfully; otherwise return false;
    bool add(const String & id, UInt16 block_id);

    /// remove `block_id` of query `id`. One query `id` can have several `blocks`
    /// return true if remove successfully; otherwise return false;
    bool remove(const String & id, UInt16 block_id);

    /// Ingest status calculation
    std::pair<String, Int32> status(const String & id) const;
    struct IngestStatus
    {
        String poll_id;
        String status;
        Int32 progress;
    };
    void getStatuses(const std::vector<String> & poll_ids, std::vector<IngestStatus> & statuses) const;

    /// number of outstanding blocks
    size_t outstandingBlocks() const;

    /// set failure code for query `id`
    void fail(const String & id, UInt16 err);

private:
    void removeExpiredBlockIds();

private:
    using SteadyClock = std::chrono::time_point<std::chrono::steady_clock>;

    struct BlockIdInfo
    {
        UInt16 total = 0;
        UInt16 err = 0;
        std::unordered_set<UInt16> ids;
    };

private:
    mutable std::shared_mutex rwlock;
    std::unordered_map<String, BlockIdInfo> blockIds;

    std::mutex lock;
    std::deque<std::pair<SteadyClock, String>> timedBlockIds;

    Int32 timeout_sec = 120;

    Poco::Logger * log;
};
}
