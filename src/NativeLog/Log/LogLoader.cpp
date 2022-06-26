#include "LogLoader.h"

#include <NativeLog/Base/Utils.h>

#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_FILE;
    extern const int INVALID_STATE;
    extern const int INVALID_SEQUENCE;
}
}

namespace nlog
{
std::pair<int64_t, int64_t> LogLoader::load(
    const fs::path & log_dir,
    const LogConfig & log_config,
    bool had_clean_shutdown,
    int64_t log_start_sn,
    int64_t recovery_point,
    std::shared_ptr<ThreadPool> adhoc_scheduler,
    Poco::Logger * logger,
    LogSegmentsPtr segments,
    LogSequenceMetadata & log_sn_meta)
{
    /// First pass: go through the files in the log directory and remove any temporary files
    /// and find any interrupted swap operations
    auto swap_files = removeTempFilesAndCollectSwapFiles(log_dir);

    /// The remaining valid swap files must come from compaction or segment split operation. We can simply
    /// rename them to regular segment files. But, before renaming, we should figure out which segments
    /// are compacted / split and delete these segment files: this is done by calculating min/maxSwapFileOffset
    auto [min_swap_file_sn, max_swap_file_sn] = minMaxSwapFileOffsets(swap_files, log_config, logger);

    /// Second pass: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As discussed above,
    /// these segments are compacted or split out but haven't been renamed to .delete before shutting down the broker
    removeSegmentFilesBetween(log_dir, min_swap_file_sn, max_swap_file_sn, logger);

    /// Third pass: rename all swap file
    for (const auto & filepath : swap_files)
    {
        LOG_INFO(logger, "Recovering file {} by renaming from {} files", filepath.c_str(), Log::SWAP_FILE_SUFFIX());
        auto new_filepath{filepath};
        fs::rename(filepath, new_filepath.replace_extension());
    }

    /// Fourth pass: load all the log and index files.
    loadSegmentFiles(log_dir, log_config, segments, logger);

    /// Calculate new recovery point ond new next sn
    int64_t new_recovery_point = 0, new_next_sn = 0;
    if (log_dir.extension().string() != Log::DELETE_DIR_SUFFIX())
    {
        std::tie(new_recovery_point, new_next_sn)
            = recoverLog(log_dir, log_config, segments, had_clean_shutdown, log_start_sn, recovery_point, adhoc_scheduler, logger);
    }
    else
    {
        if (segments->empty())
            segments->add(std::make_shared<LogSegment>(log_dir, /*base_sn*/ 0, log_config, logger));
    }

    /// calculate new log start sn, it shall be a max log sn of the first recovered segments
    /// and the start sn from checkpoint
    auto new_log_start_sn = std::max(log_start_sn, segments->firstSegment()->baseSequence());

    /// Calculate returned log sn meta
    auto active_segment = segments->lastSegment();
    log_sn_meta.record_sn = new_next_sn;
    log_sn_meta.segment_base_sn = active_segment->baseSequence();
    log_sn_meta.file_position = active_segment->size();

    return {new_log_start_sn, new_recovery_point};
}

std::vector<fs::path> LogLoader::removeTempFilesAndCollectSwapFiles(const fs::path & log_dir)
{
    std::vector<fs::path> swap_files;
    std::vector<fs::path> cleaned_files;
    int64_t min_cleaned_file_sn = std::numeric_limits<int64_t>::max();

    for (const auto & dir_entry : fs::directory_iterator{log_dir})
    {
        if (dir_entry.is_regular_file())
        {
            if (!isReadable(dir_entry.status().permissions()))
                throw DB::Exception(fmt::format("Cannot read file {}", dir_entry.path().c_str()), DB::ErrorCodes::CANNOT_READ_FILE);

            const std::string & filename = dir_entry.path().filename().string();
            if (filename.ends_with(Log::DELETED_FILE_SUFFIX()))
            {
                /// Deleting stray temporary file
                [[maybe_unused]] bool res = fs::remove(dir_entry.path());
                assert(res);
            }
            else if (filename.ends_with(Log::CLEANED_FILE_SUFFIX()))
            {
                min_cleaned_file_sn = std::min(Log::snFromFileName(filename), min_cleaned_file_sn);
                cleaned_files.push_back(dir_entry.path());
            }
            else if (filename.ends_with(Log::SWAP_FILE_SUFFIX()))
            {
                swap_files.push_back(dir_entry.path());
            }
        }
    }

    /// Delete all .swap files whose base sn is greater than the minimum .cleaned segment sn.
    /// Such .swap files could be part of an incomplete split operation that could not complete.
    std::vector<fs::path> valid_swap_files;
    for (auto & file : swap_files)
    {
        if (Log::snFromFileName(file.filename().string()) >= min_cleaned_file_sn)
        {
            /// Delete invalid swap file
            bool res = fs::remove(file);
            assert(res);
            (void)res;
        }
        else
        {
            valid_swap_files.push_back(std::move(file));
        }
    }

    for (const auto & file : cleaned_files)
    {
        /// Deleting stray .clean file
        bool res = fs::remove(file);
        assert(res);
        (void)res;
    }

    return valid_swap_files;
}

std::pair<int64_t, int64_t>
LogLoader::minMaxSwapFileOffsets(const std::vector<fs::path> & swap_files, const LogConfig & log_config, Poco::Logger * logger)
{
    int64_t min_swap_file_sn = std::numeric_limits<int64_t>::max();
    int64_t max_swap_file_sn = std::numeric_limits<int64_t>::min();
    for (const auto & file : swap_files)
    {
        auto filename = file.filename().replace_extension().string();
        /// replaceSuffix(filename, SWAP_FILE_SUFFIX, "");
        if (Log::isLogFile(filename))
        {
            auto base_sn = Log::snFromFileName(filename);
            auto segment
                = std::make_shared<LogSegment>(file.parent_path(), base_sn, log_config, logger, false, false, Log::SWAP_FILE_SUFFIX());
            LOG_INFO(
                logger,
                "Found log file {} from interrupted swap operation, which is recoverable from {} files by renaming",
                file.c_str(),
                Log::SWAP_FILE_SUFFIX());
            min_swap_file_sn = std::min(segment->baseSequence(), min_swap_file_sn);
            max_swap_file_sn = std::max(segment->readNextSequence(), max_swap_file_sn);
        }
    }
    return {min_swap_file_sn, max_swap_file_sn};
}

void LogLoader::removeSegmentFilesBetween(const fs::path & log_dir, int64_t min_sn, int64_t max_sn, Poco::Logger * logger)
{
    for (const auto & dir_entry : fs::directory_iterator{log_dir})
    {
        if (dir_entry.is_regular_file())
        {
            const auto & filepath = dir_entry.path();
            const auto & filename = filepath.filename().string();
            if (!filename.ends_with(Log::SWAP_FILE_SUFFIX()))
            {
                /// FIXME, more robust error handling, like a filename which we don't recognizes
                auto sn = Log::snFromFileName(filename);
                if (sn >= min_sn & sn < max_sn)
                {
                    LOG_INFO(logger, "Deleting segment files {} that is compacted but has not been deleted yet", filepath.c_str());
                    auto res = fs::remove(filepath);
                    assert(res);
                    (void)res;
                }
            }
        }
    }
}

/// Loads segments from disk
void LogLoader::loadSegmentFiles(const fs::path & log_dir, const LogConfig & log_config, LogSegmentsPtr segments, Poco::Logger * logger)
{
    /// Load segments in ascending order
    std::vector<fs::path> segfiles;
    for (auto & dir_entry : fs::directory_iterator{log_dir})
        if (dir_entry.is_regular_file())
            segfiles.push_back(dir_entry.path());

    std::sort(segfiles.begin(), segfiles.end());
    for (const auto & segfile : segfiles)
    {
        const auto & filename = segfile.filename().string();
        if (Log::isIndexFile(filename))
        {
            /// If it is an index file, make sure it has a corresponding .log file
            auto sn = Log::snFromFileName(filename);
            auto log_file = Log::logFile(log_dir, sn);
            if (!fs::exists(log_file))
            {
                LOG_WARNING(logger, "Found an orphaned index file {} with no corresponding log file", log_file.c_str());
                auto res = fs::remove(segfile);
                assert(res);
                (void)res;
            }
        }
        else if (Log::isLogFile(filename))
        {
            /// Load the log segment
            auto base_sn = Log::snFromFileName(filename);
            auto segment = std::make_shared<LogSegment>(log_dir, base_sn, log_config, logger, /*file_already_exists*/ true);
            assert(segment);
            auto success = segment->sanityCheck();
            if (!success)
                recoverSegment(segment);

            segments->add(segment);
        }
    }
}

/// @return the number of bytes truncated from the segment
size_t LogLoader::recoverSegment(LogSegmentPtr segment)
{
    /// FIXME
    /// auto producer_state_manager = std::make_shared<ProducerStateManager>();
    return segment->recover();
}

std::pair<int64_t, int64_t> LogLoader::recoverLog(
    const fs::path & log_dir,
    const LogConfig & log_config,
    LogSegmentsPtr segments,
    bool had_clean_shutdown,
    int64_t log_start_sn,
    int64_t recovery_point,
    std::shared_ptr<ThreadPool> adhoc_scheduler,
    Poco::Logger * logger)
{
    /// Return the log end sn if valid
    if (!had_clean_shutdown)
    {
        auto unflushed = segments->values(recovery_point, std::numeric_limits<int64_t>::max());

        LOG_INFO(logger, "Found unclean shutdown and {} unflushed segments. Recovering these segments.", unflushed.size());

        for (auto iter = unflushed.begin(), end_iter = unflushed.end(); iter != end_iter; ++iter)
        {
            size_t truncated_bytes = 0;
            try
            {
                truncated_bytes = recoverSegment(*iter);
            }
            catch (const DB::Exception & e)
            {
                if (e.code() == DB::ErrorCodes::INVALID_SEQUENCE)
                {

                    auto start_sn = (*iter)->baseSequence();
                    truncated_bytes = (*iter)->trim(start_sn);

                    LOG_ERROR(
                        logger,
                        "Found invalid sequence during recovery. Deleting the corrupt segment and creating an empty one with starting "
                        "sequence={}",
                        start_sn);
                }
            }

            if (truncated_bytes > 0)
            {
                /// We had an invalid message, delete all remaining log
                std::vector<LogSegmentPtr> to_delete(++iter, end_iter);
                removeSegmentsAsync(segments, unflushed, "data_corruption", adhoc_scheduler, logger);
                LOG_WARNING(logger, "Remove {} segments due to unclean shutdown which caused corrupted log", to_delete.size());
                break;
            }
        }
    }

    /// Delete segments if its log start is greater than log end
    std::optional<int64_t> log_end_sn_op;
    if (!segments->empty())
    {
        auto log_end_sn = segments->lastSegment()->readNextSequence();
        if (log_end_sn >= log_start_sn)
        {
            log_end_sn_op = log_end_sn;
        }
        else
        {
            LOG_WARNING(logger, "Found all logs are behind log_start_sn={} during boot, delete all of segments", log_start_sn);
            removeSegmentsAsync(segments, segments->values(), "start_sequence_breached", adhoc_scheduler, logger);
        }
    }

    if (segments->empty())
        /// No existing segments, create a new mutable segment beginning at log_start_sn
        segments->add(std::make_shared<LogSegment>(log_dir, log_start_sn, log_config, logger, false, log_config.preallocate));

    /// Update the recovery point if there was a clean shutdown and did not perform any changes to
    /// the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
    /// sn. To ensure correctness and to make it easier to reason about, it's best to only advance
    /// the recovery point when the log is flushed. If we advanced the recovery point here, we could
    /// skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery point
    /// and before we flush the segment

    if (had_clean_shutdown && log_end_sn_op.has_value())
    {
        return {log_end_sn_op.value(), log_end_sn_op.value()};
    }
    else
    {
        int64_t log_end_sn = 0;
        if (log_end_sn_op.has_value())
            log_end_sn = log_end_sn_op.value();
        else
            log_end_sn = segments->lastSegment()->readNextSequence();

        return {std::min(recovery_point, log_end_sn), log_end_sn};
    }
}

void LogLoader::removeSegmentsAsync(
    LogSegmentsPtr all_segments,
    const std::vector<LogSegmentPtr> & segments_to_remove,
    const std::string & reason,
    std::shared_ptr<ThreadPool> adhoc_scheduler,
    Poco::Logger * logger)
{
    /// First remove segments from all_segments
    for (const auto & seg : segments_to_remove)
        all_segments->remove(seg->baseSequence());

    /// Delete them from file system
    Loglet::removeSegmentFiles(segments_to_remove, true, reason, adhoc_scheduler, logger);
}
}
