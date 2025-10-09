#include <Cluster/LocalLog/Indexes/SequencePosition.h>
#include <Cluster/LocalLog/Log/LogSegment.h>
#include <Cluster/LocalLog/Log/Loglet.h>

#include <base/errnoToString.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_DELETE_FILE;
extern const int CANNOT_FLUSH_FILE;
extern const int CANNOT_TRUNCATE_FILE;
extern const int CANNOT_FSTAT;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_READ_ALL_DATA;
extern const int FILE_DOESNT_EXIST;
extern const int ATOMIC_RENAME_FAIL;
extern const int OK;
}

namespace cluster::nlog
{
LogSegment::LogSegment(
    const fs::path & log_dir,
    uint64_t segment_id_,
    int64_t base_sn_,
    LogConfigPtr log_config,
    LoggerPtr logger_,
    bool file_already_exists,
    const std::string & file_suffix)
    : id(segment_id_)
    , base_sn(base_sn_)
    , log(FileLogEntries::open(
          Loglet::logFile(log_dir, segment_id_, base_sn_, file_suffix),
          file_already_exists,
          false,
          log_config->segment_size,
          log_config->preallocate))
    , logger(logger_)
{
}

CallResult<size_t> LogSegment::append(const std::vector<ByteVector> & entry_data, size_t total_bytes)
{
    auto res = log->append(entry_data, total_bytes);
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to append, total_bytes={} appended={} {}", total_bytes, res.result, errnoToString(res.error_code));
        res.error_code = DB::ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    }
    return res;
}

CallResult<size_t> LogSegment::append(const ByteVector & entry_data)
{
    auto res = log->append(entry_data);
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to append, total_bytes={} appended={} {}", entry_data.size(), res.result, errnoToString(res.error_code));
        res.error_code = DB::ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    }
    return res;
}

CallResult<uint64_t> LogSegment::trim(uint64_t file_position)
{
    auto res = log->trim(file_position);
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to truncate log file to file_position={} error={}", file_position, errnoToString(res.error_code));
        res.error_code = DB::ErrorCodes::CANNOT_TRUNCATE_FILE;
    }

    /// For incremental flush, we will need make sure last_sync_offset is correct tracked
    last_sync_offset = std::min(last_sync_offset, file_position);

    return res;
}

int LogSegment::flush(bool incremental)
{
    /// Log sync can be very expensive: from hundred of milli-second to 10s of seconds
    auto err = incremental ? log->syncTail(last_sync_offset) : log->sync();
    if (err == 0)
    {
        LOG_DEBUG(logger, "Flushing offset={} incremental={}", last_sync_offset, incremental);
        last_sync_offset = log->size();
        return DB::ErrorCodes::OK;
    }
    else
    {
        LOG_ERROR(logger, "Failed to flush log file, error={}", errnoToString(err));
        return DB::ErrorCodes::CANNOT_FLUSH_FILE;
    }
}

CallResult<uint64_t> LogSegment::fsize() const
{
    auto res = log->fsize();
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to fstate to get log file size, error={}", errnoToString(res.error_code));
        res.error_code = DB::ErrorCodes::CANNOT_FSTAT;
    }

    return res;
}

CallResult<int64_t> LogSegment::lastModified() const
{
    auto res = log->lastModified();
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to fstate to get log file last modified time, error={}", errnoToString(res.error_code));
        res.error_code = DB::ErrorCodes::CANNOT_FSTAT;
    }
    return res;
}

int64_t LogSegment::readNextSequence(uint64_t start_position, int64_t start_sn) const
{
    bool corrupted = false;
    auto [next_sn, _] = readNextSequenceAndCheckCorruption(start_position, start_sn, corrupted);
    return next_sn;
}

/// \return {next_sn, last_entry_offset} pair and carry back if the tail entry is corrupted
std::pair<int64_t, uint64_t>
LogSegment::readNextSequenceAndCheckCorruption(uint64_t start_position, int64_t start_sn, bool & corrupted) const
{
    assert(start_sn >= base_sn);

    auto log_size = log->size();
    if (log_size == 0)
    {
        assert(start_sn == base_sn);
        return {base_sn, 0};
    }

    Stopwatch stopwatch;

    auto file_log_entries = fetch(start_position, log_size);
    if (!file_log_entries)
        return {base_sn, 0};

    LOG_DEBUG(
        logger,
        "Calculating log next sn for {}, start_sn={}, file_position={} file_size={}",
        log->getFilename().c_str(),
        start_sn,
        file_log_entries->startPosition(),
        log->size());

    int64_t read_log_entries = 0;
    uint64_t read_bytes = 0;
    int64_t last_sn = start_sn;
    uint64_t last_epoch = 0;
    uint64_t last_entry_start_offset = start_position;
    uint64_t last_entry_size = 0;

    try
    {
        file_log_entries->applyLogEntryMetadata(
            [&](cluster::EntryPtr entry, uint64_t entry_start_offset) {
                last_epoch = 1; ///  always use epoch 1
                last_sn = entry->sn;
                last_entry_start_offset = entry_start_offset;
                last_entry_size = entry->serializedSize();

                /// Since we would like to have the last batch's next sn, we always return false
                /// until it reaches the end of file
                read_log_entries += 1;
                read_bytes += last_entry_size;
                return false;
            },
            {});
    }
    catch (const DB::Exception & e)
    {
        /// Usually there are 2 exceptions : DB::ErrorCodes::CANNOT_READ_ALL_DATA and ATTEMPT_TO_READ_AFTER_EOF
        LOG_ERROR(logger, "Detected log corruption, error={}, trace={}", e.message(), e.getStackTraceString());
    }

    assert((read_log_entries == 0 && start_sn == last_sn) || (read_log_entries > 0 && read_log_entries + start_sn == last_sn + 1));

    corrupted = last_entry_start_offset + last_entry_size != log_size;

    if (corrupted)
    {
        LOG_WARNING(
            logger,
            "End of calculating log next sn for {}, elapsed={}ms, read_log_entries={}, read_bytes={}, start_sn={}, "
            "next_sn={} last_epoch={} log_size={} expected_log_size={} last_entry_size={} last_entry_start_offset={} corrupted={}",
            log->getFilename().c_str(),
            stopwatch.elapsedMilliseconds(),
            read_log_entries,
            read_bytes,
            start_sn,
            last_sn,
            last_epoch,
            log_size,
            last_entry_start_offset + last_entry_size,
            last_entry_size,
            last_entry_start_offset,
            corrupted);

        /// The last entry was corrupted, so corrupted last_sn is the new next_sn
        return {last_sn, last_entry_start_offset};
    }
    else
    {
        LOG_INFO(
            logger,
            "End of calculating log next sn for {}, elapsed={}ms, read_log_entries={}, read_bytes={}, start_sn={}, "
            "next_sn={} last_epoch={}",
            log->getFilename().c_str(),
            stopwatch.elapsedMilliseconds(),
            read_log_entries,
            read_bytes,
            start_sn,
            last_sn + 1,
            last_epoch);

        assert(last_sn >= start_sn);
        return {last_sn + 1, last_entry_start_offset};
    }
}

int LogSegment::remove()
{
    const auto & segment_file = filename();

    Stopwatch stopwatch;

    log->close();

    LOG_INFO(logger, "Removing segment={} base_sn={}", segment_file.c_str(), base_sn);
    {
        std::error_code ec;
        fs::remove_all(segment_file, ec);

        if (ec.value() != 0)
        {
            LOG_ERROR(logger, "Failed to remove segment={} base_sn={}, error={}", segment_file.c_str(), base_sn, ec.message());
            return DB::ErrorCodes::CANNOT_DELETE_FILE;
        }
    }

    LOG_INFO(
        logger, "Successfully removed segment={} base_sn={}, took {}ms", segment_file.c_str(), base_sn, stopwatch.elapsedMilliseconds());

    return DB::ErrorCodes::OK;
}

int LogSegment::changeFileSuffix(const std::string & new_suffix)
{
    const auto & file = filename();
    if (file.extension() != new_suffix)
    {
        std::error_code err;

        fs::path new_file(file);
        new_file += new_suffix;
        log->renameTo(new_file, err); /// clang-tidy: bugprone-use-after-move

        if (err.value() != 0)
        {
            LOG_ERROR(
                logger,
                "Failed to rename file from {} to {}, errcode={} message={}",
                file.c_str(),
                new_file.c_str(),
                err.value(),
                err.message());

            if (err == std::errc::no_such_file_or_directory)
                return DB::ErrorCodes::FILE_DOESNT_EXIST;

            return DB::ErrorCodes::ATOMIC_RENAME_FAIL;
        }
    }
    return DB::ErrorCodes::OK;
}

CallResult<std::pair<int64_t, size_t>> LogSegment::recover(const SequencePosition & last_sn_pos)
{
    bool corrupted = false;
    auto [next_sn, last_entry_offset]
        = readNextSequenceAndCheckCorruption(/*start_position=*/last_sn_pos.file_position, /*start_sn=*/last_sn_pos.sn, corrupted);

    if (!corrupted)
        return CallResult{std::pair<int64_t, size_t>{next_sn, 0}};

    if (auto res = log->trim(last_entry_offset); !res.hasError())
    {
        LOG_ERROR(
            logger,
            "Found corrupted log segment {}, truncated log to next_sn={} file_size={}",
            log->getFilename().c_str(),
            next_sn,
            last_entry_offset);

        assert(next_sn >= Constants::LogStartSN);
        return CallResult{std::pair<int64_t, size_t>{next_sn, res.result}};
    }
    else
    {
        LOG_ERROR(
            logger,
            "Found corrupted log segment {}, but failed to recover / truncate the corrupted data to next_sn={} at corrupted "
            "file_position={}, error={}",
            log->getFilename().c_str(),
            next_sn,
            last_entry_offset,
            errnoToString(res.error_code));

        return CallResult<std::pair<int64_t, size_t>>{DB::ErrorCodes::CANNOT_TRUNCATE_FILE};
    }
}

}
