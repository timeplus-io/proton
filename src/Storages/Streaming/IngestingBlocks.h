#pragma once

#include <Core/Types.h>
#include <base/ClockUtils.h>
#include <Poco/Logger.h>

#include <boost/core/noncopyable.hpp>

#include <chrono>
#include <deque>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <utility>


namespace DB
{
class IngestingBlocks final : public boost::noncopyable
{
public:
    IngestingBlocks(Int64 timeout_ms_, Poco::Logger * log_);
    ~IngestingBlocks() = default;

    /// One block id can have several blocks, hence sub id
    /// return true if add successfully; otherwise return false;
    // sub_block_id is calculated internally and for internal track only
    bool add(UInt64 block_id, UInt64 sub_block_id);

    /// remove `block_id` of query `id`. One query `id` can have several `blocks`
    /// return true if remove successfully; otherwise return false;
    bool remove(UInt64 block_id, UInt64 sub_block_id);

    /// Ingest status calculation
    std::pair<String, Int32> status(UInt64 block_id) const;

    struct IngestStatus
    {
        UInt64 block_id;
        String status;
        Int32 progress;

        IngestStatus(UInt64 block_id_, String status_, Int32 progress_)
            : block_id(block_id_), status(std::move(status_)), progress(progress_)
        {
        }
    };
    void getStatuses(const std::vector<UInt64> & block_ids_, std::vector<IngestStatus> & statuses) const;

    /// set failure code for query `id`
    void fail(UInt64 block_id, Int32 err);

    static UInt64 nextId();

private:
    struct BlockIdInfo
    {
        UInt64 total = 0;
        Int32 err = 0;
        /// For most of the time, we have only one ID. Use vector is faster
        /// and more memory efficient
        std::vector<UInt64> sub_ids;

        /// timestamp when this block get ingested
        Int64 ingest_timestamp_ms;

        bool add(UInt64 sub_block_id)
        {
            auto id_iter = std::find(sub_ids.begin(), sub_ids.end(), sub_block_id);
            if (id_iter == sub_ids.end())
            {
                sub_ids.push_back(sub_block_id);
                ++total;
                return true;
            }

            return false;
        }

        bool remove(UInt64 sub_block_id)
        {
            auto id_iter = std::find(sub_ids.begin(), sub_ids.end(), sub_block_id);
            if (id_iter != sub_ids.end())
            {
                sub_ids.erase(id_iter);
                return true;
            }

            return false;
        }

        bool done() const { return sub_ids.empty(); }

        UInt64 remaining() const { return sub_ids.size(); }

        explicit BlockIdInfo(UInt64 sub_block_id) : total(1), sub_ids(1, sub_block_id), ingest_timestamp_ms(MonotonicMilliseconds::now()) { }
    };

private:
    static std::atomic<uint64_t> block_id_counter;

private:
    mutable std::shared_mutex block_ids_mutex;
    std::map<UInt64, BlockIdInfo> block_ids;

    mutable std::shared_mutex expired_block_ids_mutex;
    std::set<UInt64> expired_block_ids;

    UInt64 low_watermark_block_id;
    UInt64 total_ingested = 0;

    Int64 timeout_ms;

    Poco::Logger * log;
};
}
