#include <Cluster/LocalLog/Base/Utils.h>
#include <Cluster/LocalLog/Indexes/EpochSequenceIndex.h>
#include <Cluster/LocalLog/Log/LogLoader.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_FILE;
extern const int INVALID_STATE;
extern const int INVALID_SEQUENCE;
}
}

namespace cluster::nlog
{
LogSequenceMetadata LogLoader::load(
    const fs::path & log_dir,
    const LogConfigPtr & log_config,
    bool had_clean_shutdown,
    const EpochSequenceIndexPtr & leader_epoch_index,
    const TimestampSequenceIndexPtr & event_ts_sn_index,
    const SequencePositionIndexPtr & sn_pos_index,
    LogSegmentsPtr segments,
    LoggerPtr logger)
{
    /// First pass: go through the files in the log directory and remove any temporary files
    /// and find any interrupted swap operations
    removeTempFiles(log_dir, event_ts_sn_index, sn_pos_index, logger);

    loadSegmentFiles(log_dir, log_config, segments, logger);

    int64_t next_sn = 0;

    if (!segments->empty())
    {
        /// Calculate new recovery point ond new next sn
        if (log_dir.extension().string() != Log::DELETE_DIR_SUFFIX())
            next_sn = recoverLog(leader_epoch_index, event_ts_sn_index, sn_pos_index, segments, had_clean_shutdown, logger);
    }
    else
    {
        segments->add(std::make_shared<LogSegment>(
            log_dir, /*segment_id_=*/Constants::SegmentStartID, /*base_sn_=*/Constants::LogStartSN, log_config, logger));

        next_sn = Constants::LogStartSN;
    }

    assert(segments && !segments->empty());
    auto active_segment = segments->lastSegment();

    return LogSequenceMetadata(
        /*epoch_=*/1, /// mode always uses epoch 1
        /*entry_sn_=*/next_sn,
        /*segment_base_sn_=*/active_segment->baseSequence(),
        /*file_position_=*/active_segment->size());
}

void LogLoader::removeTempFiles(
    const fs::path & log_dir,
    const TimestampSequenceIndexPtr & event_ts_sn_index,
    const SequencePositionIndexPtr & sn_pos_index,
    LoggerPtr logger)
{
    for (const auto & dir_entry : fs::directory_iterator{log_dir})
    {
        if (dir_entry.is_regular_file())
        {
            if (!isReadable(dir_entry.status().permissions()))
                throw DB::Exception(DB::ErrorCodes::CANNOT_READ_FILE, "Cannot read file {}", dir_entry.path().c_str());

            const std::string & filename = dir_entry.path().filename().string();
            if (filename.ends_with(Log::DELETED_FILE_SUFFIX()))
            {
                /// Deleting stray temporary file
                [[maybe_unused]] bool res = fs::remove(dir_entry.path());
                assert(res);

                /// We always delete at head and truncate at tail
                auto [_, sn] = Log::segmentIDAndSequenceFromFileName(filename);
                /// We still have garbage left in indexes, will clean them up
                /// after next segment
                if (auto err = event_ts_sn_index->trimHead(sn); err != DB::ErrorCodes::OK)
                    LOG_ERROR(
                        logger,
                        "Failed truncate event timestamp and sn index to head sn={} for log={}, error={}",
                        sn,
                        filename,
                        DB::ErrorCodes::getName(err));

                if (auto err = sn_pos_index->trimHead(sn); err != DB::ErrorCodes::OK)
                    LOG_ERROR(
                        logger,
                        "Failed truncate sn and log position index to head sn={} for log={}, error={}",
                        sn,
                        filename,
                        DB::ErrorCodes::getName(err));
            }
        }
    }
}

/// Loads segments from disk
void LogLoader::loadSegmentFiles(const fs::path & log_dir, const LogConfigPtr & log_config, LogSegmentsPtr segments, LoggerPtr logger)
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
        if (Log::isLogFile(filename))
        {
            /// Load the log segment
            auto res = Log::segmentIDAndSequenceFromFileName(filename);
            if (res.hasError())
            {
                LOG_ERROR(logger, "Invalid log segment filename={}, ignore it", filename);
                continue;
            }

            auto [segment_id, base_sn] = res.result;
            auto segment = std::make_shared<LogSegment>(log_dir, segment_id, base_sn, log_config, logger, /*file_already_exists=*/true);
            segments->add(segment);
        }
    }
}

/// Recover the log segments if there was an unclean shutdown. Ensures there is at least one
/// active segment, and returns the updated recovery point and next sequence number after recovery.
/// Along the way, the method updates the SequenceEpochIndex
int64_t LogLoader::recoverLog(
    const EpochSequenceIndexPtr & leader_epoch_index,
    const TimestampSequenceIndexPtr & event_ts_sn_index,
    const SequencePositionIndexPtr & sn_pos_index,
    LogSegmentsPtr segments,
    bool had_clean_shutdown,
    LoggerPtr logger)
{
    (void)leader_epoch_index;

    assert(segments && !segments->empty());

    auto get_last_sn_pos = [&](const LogSegmentPtr & last_segment) {
        auto sn_pos = sn_pos_index->lastIndexedSequencePosition();
        if (sn_pos.hasError())
        {
            LOG_ERROR(logger, "Failed to get last indexed sequence position, error={}", sn_pos.errorString());
            throw DB::Exception(sn_pos.error_code, "Failed to get last indexed sequence position");
        }

        if (!sn_pos.result || sn_pos.result->sn < last_segment->baseSequence())
        {
            /// If last indexed sn is not in the active (last) segment,
            /// use the last segment's base sn
            sn_pos.result = {last_segment->baseSequence(), /*file_position_=*/0};
        }
        else
        {
            /// Decode the global file position to segment id and real file position of the segment
            auto [segment_id, file_position] = Loglet::segmentIDAndFilePosition(sn_pos.result->file_position);
            assert(last_segment->getID() == segment_id);
            sn_pos.result->file_position = file_position;
        }

        return *sn_pos.result;
    };

    /// Return the log end sn if valid
    if (!had_clean_shutdown)
    {
        /// We only need recover last segment since when segment roll, we will flush the rolled segment
        auto last_segment = segments->lastSegment();
        auto last_sn_pos = get_last_sn_pos(last_segment);

        auto recover_result = last_segment->recover(last_sn_pos);
        if (recover_result.hasError())
            throw DB::Exception(recover_result.error_code, "Failed to recover segment : {}", last_segment->filename().c_str());

        auto [next_sn, trimmed_bytes] = recover_result.result;
        if (trimmed_bytes > 0)
        {
            event_ts_sn_index->trim(next_sn);
            sn_pos_index->trim(next_sn);
        }

        return next_sn;
    }
    else
    {
        auto active = segments->lastSegment();
        auto sn_pos = get_last_sn_pos(active);

        /// Follow the tail of the log and find the next sn
        return active->readNextSequence(/*start_position=*/sn_pos.file_position, /*start_sn=*/sn_pos.sn);
    }
}

void LogLoader::removeSegmentsSync(
    LogSegmentsPtr all_segments,
    const std::vector<LogSegmentPtr> & segments_to_remove,
    const TimestampSequenceIndexPtr & event_ts_sn_index,
    const SequencePositionIndexPtr & sn_pos_index,
    std::optional<int64_t> new_start_sn,
    std::string_view reason,
    LoggerPtr logger)
{
    /// First remove segments from all_segments
    for (const auto & seg : segments_to_remove)
        all_segments->remove(seg->baseSequence());

    if (!new_start_sn)
    {
        assert(!all_segments->empty());
        new_start_sn = all_segments->lastSegment()->baseSequence();
    }
    else
    {
        assert(all_segments->empty());
    }

    assert(new_start_sn);

    /// Delete them from file system
    Loglet::removeSegmentFilesSync(segments_to_remove, event_ts_sn_index, sn_pos_index, *new_start_sn, reason, logger);
}
}
