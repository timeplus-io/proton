#pragma once

#include <Cluster/LocalLog/Indexes/PersistentMonoMap.h>
#include <Cluster/LocalLog/Indexes/TimestampSequence.h>

#include <mutex>

namespace cluster::nlog
{
class TimestampSequenceIndex final
{
public:
    /// \param file_max_size_ when log file reached this setting, it will get rolled automatically
    TimestampSequenceIndex(const fs::path & ckpt_dir, uint64_t file_max_size_, bool preallocate_, LoggerPtr logger_)
        : timestamp_sn_map(ckpt_dir / "ts-sn.idx", file_max_size_, preallocate_, "ts-pos", logger_)
    {
    }

    CallResult<bool> maybeIndex(TimestampSequence ts_sn, bool sync = true)
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.maybeIndex(ts_sn, sync);
    }

    CallResult<std::optional<TimestampSequence>> lastIndexedTimestampSequence()
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.last();
    }

    /// Find the earliest entry whose timesstamp is less than or equal to (<=) the given ts
    /// \param ts The timestamp to lookup
    /// \return The found timestamp and the corresponding sn
    ///         If the timestamp to be found is smaller than the earliest entry in the index (or the index is empty)
    ///         {} is returned
    CallResult<std::optional<TimestampSequence>> leftLowerBoundSequenceForTimestamp(int64_t ts)
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.leftLowerBound(ts);
    }

    /// Truncate tail indexed entries which has sequence in [sn, ...)
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trim(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.trimByValue(sn);
    }

    /// Truncate head indexed entries which has sequence in [..., sn]
    /// Best effort, may be trim a prefix sub-range of [..., sn].
    /// Please note, trimHead never trim the current active log segment,
    /// since trimHead for now it is used for garbage collection
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimHead(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.trimHeadByValue(sn);
    }

    /// Delete all indexed entries
    int remove()
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.remove();
    }

    int close()
    {
        std::scoped_lock lock{mu};
        return timestamp_sn_map.close();
    }

private:
    mutable std::mutex mu;
    PersistentMonoMap<TimestampSequence> timestamp_sn_map;
};

using TimestampSequenceIndexPtr = std::unique_ptr<TimestampSequenceIndex>;
}
