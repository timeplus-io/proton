#include "IngestingBlocks.h"

#include <base/logger_useful.h>


namespace DB
{
IngestingBlocks::IngestingBlocks(Poco::Logger * log_, Int32 timeout_sec_) : timeout_ms(timeout_sec_ * 1000), log(log_)
{
}

bool IngestingBlocks::add(UInt64 block_id, UInt64 sub_block_id)
{
    bool added = false;
    UInt64 ingested = 0;
    UInt64 outstanding = 0;
    UInt64 committed_block_id = 0;
    std::vector<std::tuple<UInt64, UInt64, UInt64>> expired;
    {
        std::unique_lock guard(block_ids_mutex);

        auto iter = block_ids.find(block_id);
        if (iter == block_ids.end())
        {
            block_ids.emplace(block_id, sub_block_id);
            added = true;
        }
        else
        {
            /// There may be several sub-batches one ID
            added = iter->second.add(sub_block_id);
        }

        total_ingested += added;
        if (total_ingested % 1000 == 0)
        {
            ingested = total_ingested;
            committed_block_id = low_watermark_block_id;
            outstanding = block_ids.size();
        }

        /// Remove expired block ids
        /// (block_id, total, remaining)
        auto now = MonotonicMilliseconds::now();

        /// Here we assume the ingest timestamp of a block with bigger block ID is approximately bigger than
        /// the one with smaller block ID. This is not always true, but it is OK if it is not.
        for (iter = block_ids.begin(); iter != block_ids.end();)
        {
            /// After removing a timed out block, we may end up with lots of committed blocks,
            /// remove them as well to progress low watermark
            if (now - iter->second.ingest_timestamp_ms >= timeout_ms || iter->second.done())
            {
                expired.emplace_back(iter->first, iter->second.total, iter->second.remaining());
                /// Progress the low watermark
                low_watermark_block_id = iter->first;
                iter = block_ids.erase(iter);
            }
            else
            {
                break;
            }
        }
    }

    for (const auto & p : expired)
        /// FIXME, introduce an expired map
        LOG_WARNING(
            log,
            "Timed out and removed. There were {} / {} pending data blocks waiting to be committed for block_id={}",
            std::get<2>(p),
            std::get<1>(p),
            std::get<0>(p));


    if (ingested)
        LOG_INFO(log, "IngestingBlocks accepted={}, outstanding={}, committed_block_id={}", ingested, outstanding, committed_block_id);

    return added;
}

bool IngestingBlocks::remove(UInt64 block_id, UInt64 sub_block_id)
{
    bool removed = false;
    UInt64 committed_block_id = 0;
    UInt64 outstanding = 0;

    {
        std::unique_lock guard(block_ids_mutex);
        auto iter = block_ids.find(block_id);
        if (iter != block_ids.end())
        {
            removed = iter->second.remove(sub_block_id);
            if (iter->second.done() && iter == block_ids.begin())
            {
                /// If all of the subblocks in a block is done and if it is the first block in the ingesting blocks,
                /// remove it from block_ids map to progress the low watermark of the committed block ID. Otherwise
                /// we keep the block in the block_ids map to serve query status
                low_watermark_block_id = block_id;
                iter = block_ids.erase(iter);

                /// We need further loop the map to see if the next block is done as well since remove can be out of order
                for (; iter != block_ids.end();)
                {
                    if (iter->second.done())
                        iter = block_ids.erase(iter);
                    else
                        break;
                }
            }
        }

        committed_block_id = low_watermark_block_id;
        outstanding = block_ids.size();
    }

    if (outstanding == 0)
        LOG_INFO(
            log,
            "Removed={} block_id={} sub_block_id={} outstanding={} committed_block_id={}",
            removed,
            block_id,
            sub_block_id,
            outstanding,
            committed_block_id);

    return removed;
}

std::pair<String, Int32> IngestingBlocks::status(UInt64 block_id) const
{
    std::shared_lock guard(block_ids_mutex);

    /// FIXME, we actually need lookup expired map to see if it is there
    if (block_id <= low_watermark_block_id)
        return {"Succeeded", 100};

    auto iter = block_ids.find(block_id);
    if (iter != block_ids.end())
    {
        if (iter->second.err != 0)
            return {"Failed", ((iter->second.total - iter->second.remaining()) * 100) / iter->second.total};

        if (iter->second.done())
            return {"Succeeded", 100};
        else
            return {"Processing", ((iter->second.total - iter->second.remaining()) * 100) / iter->second.total};
    }

    return {"Unknown", -1};
}

void IngestingBlocks::getStatuses(const std::vector<UInt64> & block_ids_, std::vector<IngestingBlocks::IngestStatus> & statuses) const
{
    for (auto block_id : block_ids_)
    {
        auto block_status = status(block_id);
        statuses.emplace_back(block_id, std::move(block_status.first), block_status.second);
    }
}

void IngestingBlocks::fail(UInt64 block_id, Int32 err)
{
    std::unique_lock guard(block_ids_mutex);
    auto iter = block_ids.find(block_id);
    if (iter != block_ids.end())
        iter->second.err = err;
}

std::atomic<uint64_t> IngestingBlocks::block_id_counter = 0;

uint64_t IngestingBlocks::nextId()
{
    return block_id_counter++;
}
}
