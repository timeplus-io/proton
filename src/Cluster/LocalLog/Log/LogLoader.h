#pragma once

#include <Cluster/LocalLog/Log/Log.h>

namespace cluster::nlog
{
/// This class is responsible for all activities related with recovery of log segments from disk
class LogLoader final
{
public:
    /// \param log_dir The directory from which log segments need to be loaded
    /// \param log_config The configuration settings for the log being loaded
    /// \param had_clean_shutdown The boolean flag to indicate whether the associated log previously had a clean shutdown
    /// \param segments The Segments instance into which segments recovered from disk will be populated
    /// \return Next LogSequenceMetadata for the log
    static LogSequenceMetadata load(
        const fs::path & log_dir,
        const LogConfigPtr & log_config,
        bool had_clean_shutdown,
        const EpochSequenceIndexPtr & leader_epoch_ckpt,
        const TimestampSequenceIndexPtr & event_ts_sn_index,
        const SequencePositionIndexPtr & sn_pos_index,
        LogSegmentsPtr segments,
        LoggerPtr logger);

private:
    /// Removes any temporary files found in the log directory, and creates a list of all .swap files which could be swapped
    /// in place of existing segments. For log splitting, we know that any .swap file whose base sn is higher than
    static void removeTempFiles(
        const fs::path & log_dir,
        const TimestampSequenceIndexPtr & event_ts_sn_index,
        const SequencePositionIndexPtr & sn_pos_index,
        LoggerPtr logger);

    /// Loads log segments from disk
    static void
    loadSegmentFiles(const fs::path & log_dir, const LogConfigPtr & log_config, LogSegmentsPtr segments_, LoggerPtr logger);

    /// Recover the log segments if there was an unclean shutdown. Ensures there is at least one
    /// active segment, and returns the updated recovery point and next sn after recovery.
    /// Along the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager
    /// \return next_sn of the log
    static int64_t recoverLog(
        const EpochSequenceIndexPtr & leader_epoch_index,
        const TimestampSequenceIndexPtr & event_ts_sn_index,
        const SequencePositionIndexPtr & sn_pos_index,
        LogSegmentsPtr segments_,
        bool had_clean_shutdown,
        LoggerPtr logger);

    /// This method deletes the given log segments and the associated producer snapshots, by doing the following
    /// for each of them
    static void removeSegmentsSync(
        LogSegmentsPtr all_segments,
        const std::vector<LogSegmentPtr> & segments_to_remove,
        const TimestampSequenceIndexPtr & event_ts_sn_index,
        const SequencePositionIndexPtr & sn_pos_index,
        std::optional<int64_t> new_start_sn,
        std::string_view reason,
        LoggerPtr logger);
};
}
