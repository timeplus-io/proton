#pragma once

#include <NativeLog/Base/Stds.h>
#include <NativeLog/Common/TopicShard.h>

#include <memory>

namespace nlog
{

class ProducerStateManager final
{
public:
    ProducerStateManager(const TopicShard & topic_shard_, const fs::path & log_dir_, int32_t max_producer_id_expiration_ms_)
        : topic_shard(topic_shard_), max_producer_id_expiration_ms(max_producer_id_expiration_ms_)
    {
        (void)max_producer_id_expiration_ms;
        (void)log_dir_;
    }

    bool empty() const
    {
        /// FIXME
        return true;
    }

    /// Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping
    void truncateFullyAndStartAt(int64_t offset)
    {
        (void)offset;
    }

    /// Acknowledge all transactions which have been completed before a given offset.
    /// This allows the LSO to advance to the next unstable offset.
    void onHighWatermarkUpdated(int64_t high_watermark)
    {
        (void)high_watermark;
    }

    /// Update the parent dir for this state manager and all of the snapshot
    /// files which it manages
    void updateParentDir(const fs::path & dir)
    {
        (void) dir;
    }

private:
    void removeStraySnapshots(const std::vector<int64_t> & segment_base_offsets)
    {
        (void)segment_base_offsets;
    }

    friend class LogLoader;

private:
    TopicShard topic_shard;
    int32_t max_producer_id_expiration_ms;
};

using ProducerStateManagerPtr = std::shared_ptr<ProducerStateManager>;
}
