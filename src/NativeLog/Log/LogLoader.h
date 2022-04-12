#pragma once

#include "Log.h"

namespace nlog
{
/// This class is responsible for all activities related with recovery of log segments from disk
class LogLoader final
{
public:
    /// @param log_dir The directory from which log segments need to be loaded
    /// @param log_config The configuration settings for the log being loaded
    /// @param had_clean_shutdown The boolean flag to indicate whether the associated log previously had a clean shutdown
    /// @param log_start_sn The checkpoint of the log start sn
    /// @param recovery_point The checkpoint of the sn at which to begin the recovery
    /// @param segments_ The Segments instance into which segments recovered from disk will be populated
    /// @param log_sn_meta_ returned newly calculated LogOffsetMeta
    /// @return newly calculated {start_sn, recovery_point} pair
    static std::pair<int64_t, int64_t> load(
        const fs::path & log_dir,
        const LogConfig & log_config,
        bool had_clean_shutdown,
        int64_t log_start_sn,
        int64_t recovery_point,
        Poco::Logger * logger_,
        LogSegmentsPtr segments_,
        LogSequenceMetadata & log_sn_meta_);

private:
    /// Removes any temporary files found in the log directory, and creates a list of all .swap files which could be swapped
    /// in place of existing segments. For log splitting, we know that any .swap file whose base sn is higher than
    /// the smallest sn. .clean file could be part of an incomplete split operation. Such .swap files are also deleted by this method
    /// @return A list of .swap files that are valid to be swapped in as segment files and index files
    static std::vector<fs::path> removeTempFilesAndCollectSwapFiles(const fs::path & log_dir);

    static std::pair<int64_t, int64_t> minMaxSwapFileOffsets(const std::vector<fs::path> & swap_files, const LogConfig & log_config, Poco::Logger * logger);

    static void removeSegmentFilesBetween(const fs::path & log_dir, int64_t min_sn, int64_t max_sn, Poco::Logger * logger);

    /// Loads segments from disk
    static void loadSegmentFiles(const fs::path & log_dir, const LogConfig & log_config, LogSegmentsPtr segments_, Poco::Logger * logger);

    /// @return the number of bytes truncated from the segment
    static int32_t recoverSegment(LogSegmentPtr segment);

    /// Recover the log segments if there was an unclean shutdown. Ensures there is at least one
    /// active segment, and returns the updated recovery point and next sn after recovery.
    /// Along the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager
    /// @return a pair containing (new_recovery_point, next_sn)
    static std::pair<int64_t, int64_t> recoverLog(
        const fs::path & log_dir,
        const LogConfig & log_config,
        LogSegmentsPtr segments_,
        bool had_clean_shutdown,
        int64_t log_start_sn,
        int64_t recovery_point,
        Poco::Logger * logger);

    /// This method deletes the given log segments and the associated producer snapshots, by doing the following
    /// for each of them
    /// - It removes the segment from the segment map so that it will no longer be used for reads
    /// - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
    ///   Synchronization and without the possibility of physically deleting a file while it is being read
    static void removeSegmentsAsync(const std::vector<LogSegmentPtr> & segments);
};
}
