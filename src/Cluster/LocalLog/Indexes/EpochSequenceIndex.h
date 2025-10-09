#pragma once

#include <Cluster/Common/EpochSequence.h>
#include <Cluster/LocalLog/Indexes/PersistentMonoMap.h>

#include <mutex>

namespace cluster::nlog
{
class EpochSequenceIndex final
{
public:
    /// \param file_max_size_ when log file reached this setting, it will get rolled automatically
    EpochSequenceIndex(const fs::path & ckpt_dir, uint64_t file_max_size_, bool preallocate_, LoggerPtr logger_)
        : epoch_sn_map(ckpt_dir / "epoch-sn.idx", file_max_size_, preallocate_, "epoch-sn", logger_)
    {
    }

    /// \return true if updated / inserted otherwise false, error code if something wrong happened
    CallResult<bool> maybeIndexEpochStartSequence(EpochSequence epoch_sn, bool sync = true)
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.maybeIndex(epoch_sn, sync);
    }

    /// Find the first leader epoch whose starting sequence is less or equal to parameter sn
    /// \return the leader epoch sequence if found, otherwise empty result or error code is returned
    CallResult<std::optional<EpochSequence>> leaderEpochSequenceFor(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.leftLowerBoundByValue(sn);
    }

    CallResult<std::optional<EpochSequence>> latestLeaderEpochSequence()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.last();
    }

    CallResult<std::optional<EpochSequence>> previousLeaderEpochSequence()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.prevLast();
    }

    /// For convenience
    CallResult<std::optional<uint64_t>> latestLeaderEpoch()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.lastKey();
    }

    CallResult<std::optional<uint64_t>> previousLeaderEpoch()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.prevLastKey();
    }

    /// For testing
    CallResult<std::optional<EpochSequence>> earliestLeaderEpochSequence()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.first();
    }

    /// Truncate tail indexed entries which has sequence in [sn, ...)
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trim(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.trimByValue(sn);
    }

    /// Truncate head indexed entries which has sequence in [..., sn)
    /// Best effort, may be trim a prefix sub-range of [..., sn].
    /// Please note, trimHead never trim the current active log segment, it is  find
    /// since trimHead for now it is used for garbage collection
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimHead(int64_t sn)
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.trimHeadByValue(sn);
    }

    /// Delete all indexed entries
    int remove()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.remove();
    }

    int close()
    {
        std::scoped_lock lock{mu};
        return epoch_sn_map.close();
    }

private:
    mutable std::mutex mu;
    PersistentMonoMap<EpochSequence> epoch_sn_map;
};

using EpochSequenceIndexPtr = std::unique_ptr<EpochSequenceIndex>;
}
