#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Log/FileLogSource.h>
#include <Storages/ExternalStream/Log/fileLastModifiedTime.h>

#include <Storages/ExternalStream/BreakLines.h>
#include <base/ClockUtils.h>
#include <Common/logger_useful.h>

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>


namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSTAT;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

FileLogSource::FileLogSource(
    FileLog * file_log_,
    Block header_,
    ContextPtr query_context_,
    size_t max_block_size_,
    Int64 start_timestamp_,
    FileLogSource::FileContainer files_,
    Poco::Logger * log_)
    : Streaming::ISource(header_, true, ProcessorID::FileLogSourceID)
    , file_log(file_log_)
    , query_context(query_context_)
    , column_type(header_.getByPosition(0).type)
    , head_chunk(header_.getColumns(), 0)
    , max_block_size(max_block_size_)
    , start_timestamp(start_timestamp_)
    , files(std::move(files_))
    , log(log_)
{
    iter = files.begin();

    last_flush_ms = MonotonicMilliseconds::now();
    last_check = last_flush_ms;

    (void)max_block_size;
    (void)start_timestamp;
}

FileLogSource::~FileLogSource()
{
    if (current_fd >= 0)
        ::close(current_fd);
}

Chunk FileLogSource::generate()
{
    if (iter != files.end())
    {
        if (current_fd < 0)
        {
            current_fd = ::open(iter->second.c_str(), O_RDONLY);
            if (current_fd < 0)
                throwFromErrnoWithPath(
                    "Cannot open file ",
                    iter->second.string(),
                    errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
                    errno);

            current_offset = 0;

            if (buffer.empty())
                buffer.resize(4 * 1024 * 1024);

            if (!handleCurrentFile())
                return head_chunk.clone();
        }

        return readAndProcess();
    }
    else
    {
        checkNewFiles();
    }

    return head_chunk.clone();
}

Chunk FileLogSource::readAndProcess()
{
    assert(current_fd >= 0);

    auto n = ::pread(current_fd, buffer.data() + buffer_offset, buffer.size() - buffer_offset, current_offset);
    if (n > 0)
    {
        current_offset += n;
        buffer_offset += n;
        remaining += n;

        /// When there are data for the current file, update last_check to avoid future new file check
        last_check = MonotonicMilliseconds::now();

        return process();
    }
    else if (n == 0)
    {
        /// EOF
        if (files.size() == 1)
        {
            checkNewFiles();
            return head_chunk.clone();
        }
        else
        {
            /// Project what we have in the buffer and move to the next log file
            iter = files.erase(iter);

            ::close(current_fd);
            current_fd = -1;

            return flushBuffer();
        }
    }
    else
    {
        if (errno != EINTR)
            DB::throwFromErrnoWithPath("Cannot read from file ", iter->second.string(), DB::ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
    }

    return head_chunk.clone();
}

Chunk FileLogSource::process()
{
    assert(buffer_offset >= remaining);
    auto left = remaining;
    auto lines{breakLines(buffer.data() + (buffer_offset - left), left, file_log->linebreakerRegex(), 4096)};

    if (!lines.empty())
    {
        auto col = column_type->createColumn();
        for (const auto & line : lines)
            col->insertData(line.data(), line.size());

        /// Move the remaining bytes to the start of buffer
        std::memmove(buffer.data(), buffer.data() + (buffer_offset - left), left);
        remaining = left;
        buffer_offset = left;

        return Chunk(Columns(1, std::move(col)), lines.size());
    }
    else
    {
        assert(left == remaining);
        return head_chunk.clone();
    }
}

Chunk FileLogSource::flushBuffer()
{
    if (remaining > 0)
    {
        auto col = column_type->createColumn();
        col->insertData(buffer.data() + buffer_offset, remaining);
        remaining = 0;
        buffer_offset = 0;
        return Chunk(Columns{1, std::move(col)}, 1);
    }

    return head_chunk.clone();
}

void FileLogSource::checkNewFiles()
{
    if (MonotonicMilliseconds::now() - last_check <= check_interval_ms)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return;
    }

    auto new_files{file_log->searchForCandidates()};
    last_check = MonotonicMilliseconds::now();

    if (new_files.empty())
    {
        /// FIXME, notification
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    else
    {
        std::vector<char> read_buf;
        read_buf.resize(file_log->hashBytes());

        const auto & filename = iter->second.string();

        int first_new_fd = -1;
        for (auto new_iter = new_files.begin(); new_iter != new_files.end();)
        {
            auto new_fd = ::open(new_iter->second.c_str(), O_RDONLY);
            if (new_fd < 0)
            {
                new_iter = new_files.erase(new_iter);
                LOG_ERROR(log, "Failed to open log file {}. Skip analyzing it for now", filename);
                continue;
            }

            auto hash_val = calculateFileHash(new_fd, read_buf, read_buf.size(), filename);
            if (file_hashes.contains(hash_val))
            {
                new_iter = new_files.erase(new_iter);
                ::close(new_fd);
                LOG_INFO(log, "Found handled log file {}. Skip analyzing it", filename);
            }
            else
            {
                ++new_iter;
                file_hashes.emplace(hash_val, filename);

                if (first_new_fd < 0)
                    first_new_fd = new_fd;
                else
                    ::close(new_fd);

                LOG_INFO(log, "Found new log file {} ", filename);
            }
        }

        if (!new_files.empty())
        {
            LOG_INFO(log, "Closing current log file {} and progress to {}", iter->second.c_str(), new_files.begin()->second.c_str());

            ::close(current_fd);
            assert(first_new_fd >= 0);
            current_fd = first_new_fd;
            current_offset = 0;

            files.swap(new_files);
            iter = files.begin();
        }
    }
}

bool FileLogSource::handleCurrentFile()
{
    assert(current_fd >= 0);

    const auto & filename = iter->second.string();

    auto hash_val = calculateFileHash(current_fd, buffer, file_log->hashBytes(), filename);
    auto [hash_iter, inserted] = file_hashes.try_emplace(hash_val, filename);
    if (!inserted)
    {
        ::close(current_fd);
        current_fd = -1;

        LOG_INFO(log, "Log file was handled, prev_name={} current_name={} hash={}", hash_iter->second, filename, hash_val);
        iter = files.erase(iter);
        return false;
    }

    LOG_INFO(log, "Processing log file {}, hash={} hashed_bytes={}", filename, hash_val, 1024);

    return true;
}

size_t FileLogSource::calculateFileHash(int fd, std::vector<char> & read_buf, size_t bytes_to_read, const String & filename)
{
    size_t read_bytes = 0;

    assert(read_buf.size() >= bytes_to_read);

    while (bytes_to_read && !isCancelled())
    {
        auto n = ::pread(fd, read_buf.data(), bytes_to_read, read_bytes);
        if (n > 0)
        {
            read_bytes += n;
            bytes_to_read -= n;
        }
        else if (n == 0)
        {
            /// EOF
            LOG_INFO(log, "Wait for more data in file={} to calculate its hash", filename);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        else
        {
            if (errno != EINTR)
                DB::throwFromErrnoWithPath("Cannot read from file", filename, DB::ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno);
        }
    }

    return std::hash<std::string_view>{}(std::string_view(read_buf.data(), read_bytes));
}
}
