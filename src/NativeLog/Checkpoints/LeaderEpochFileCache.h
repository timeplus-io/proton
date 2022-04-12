#pragma once

#include "LeaderEpochCheckpointFile.h"

#include <shared_mutex>

namespace nlog
{
/// Represents a cache of (LeaderEpoch => Offset) mappings for a particular replica
/// Leader Epoch = epoch assigned to each leader by the controller
/// Offset = offset of the first message in each epoch
class LeaderEpochFileCache final
{
public:
    LeaderEpochFileCache(const StreamShard & stream_shard_, LeaderEpochCheckpointFilePtr checkpoint_)
        : stream_shard(stream_shard_), checkpoint(checkpoint_)
    {
        assert(checkpoint);
    }

    /// Remove all epoch entries from the store with start offsets greater than or equal to the passed offset.
    void truncateFromEnd(int64_t end_offset)
    {
        std::unique_lock guard{epoch_lock};
        (void) end_offset;
    }

    /// Clears old epoch entries. This method searches for the oldest epoch < offset,
    /// updates the saved epoch offset to be offset, then clears any previous epoch entries
    /// This method is exclusive: so truncateFromStart(6) will retain an entry at offset 6
    void truncateFromStart(int64_t start_offset)
    {
        std::unique_lock guard{epoch_lock};
        (void) start_offset;
    }

    /// Delete all entries
    void clearAndFlush()
    {
        std::unique_lock guard{epoch_lock};
    }

    void clear()
    {
        std::unique_lock guard{epoch_lock};
    }

private:
    StreamShard stream_shard;
    LeaderEpochCheckpointFilePtr checkpoint;

    std::shared_mutex epoch_lock;
};

using LeaderEpochFileCachePtr = std::shared_ptr<LeaderEpochFileCache>;
}
