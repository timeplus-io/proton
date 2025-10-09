#pragma once

#include <Cluster/LocalLog/Indexes/PersistentMonoMap.h>
#include <Cluster/LocalLog/Indexes/SequencePosition.h>

#include <mutex>

namespace cluster::nlog
{
class SequencePositionIndex final
{
public:
    /// \param file_max_size_ when log file reached this setting, it will get rolled automatically
    SequencePositionIndex(const fs::path & ckpt_dir, uint64_t file_max_size_, bool preallocate_, LoggerPtr logger_)
        : sn_position_map(ckpt_dir / "sn-pos.idx", file_max_size_, preallocate_, "sn-pos", logger_)
    {
    }

    CallResult<bool> maybeIndex(SequencePosition sn_pos, bool sync = true)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.maybeIndex(sn_pos, sync);
    }

    CallResult<std::optional<SequencePosition>> lastIndexedSequencePosition()
    {
        std::scoped_lock lock{mu};
        return sn_position_map.last();
    }

    /// For testing
    CallResult<std::optional<SequencePosition>> firstIndexedSequencePosition()
    {
        std::scoped_lock lock{mu};
        return sn_position_map.first();
    }

    /// Find the "latest" entry whose sn less than or equal to (<=) the given sn.
    /// \param sn The sn to lookup
    /// \return The sn found and the corresponding position for this sn.
    ///         If the target sn is smaller than the least entry in the index (or the index is empty)
    ///         {} is returned
    CallResult<std::optional<SequencePosition>> leftLowerBoundPositionForSequence(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.leftLowerBound(sn);
    }

    /// Find the "earliest" file position indexed is less than or equal to (<=) the given position
    /// \param position The physical position to lookup
    /// \return The found file position and its sn. If the given position pass the end of segment, {} will be returned
    CallResult<std::optional<SequencePosition>> leftLowerBoundSequenceForPosition(int64_t position)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.leftLowerBoundByValue(position);
    }

    /// Find the "earliest" file position indexed is equal to or greater than (>=) the given position
    /// \param position The physical position to lookup
    /// \return The found file position and its sn. If the given position pass the end of segment, {} will be returned
    CallResult<std::optional<SequencePosition>> lowerBoundSequenceForPosition(int64_t position)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.lowerBoundByValue(position);
    }

    /// Truncate tail indexed entries which has sequence in [sn, ...)
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trim(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.trim(sn);
    }

    /// Truncate head indexed entries which has sequence in [..., sn)
    /// Best effort, may be trim a prefix sub-range of [..., sn].
    /// Please note, trimHead never trim the current active log segment, it is  find
    /// since trimHead for now it is used for garbage collection
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimHead(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return sn_position_map.trimHead(sn);
    }

    /// Delete all indexed entries
    int remove()
    {
        std::scoped_lock lock{mu};
        return sn_position_map.remove();
    }

    int close()
    {
        std::scoped_lock lock{mu};
        return sn_position_map.close();
    }

private:
    mutable std::mutex mu;
    PersistentMonoMap<SequencePosition> sn_position_map;
};

using SequencePositionIndexPtr = std::unique_ptr<SequencePositionIndex>;
}
