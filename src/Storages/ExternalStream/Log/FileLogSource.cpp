#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Log/FileLogSource.h>
#include <Storages/ExternalStream/Log/fileLastModifiedTime.h>

#include <Storages/ExternalStream/BreakLines.h>
#include <base/ClockUtils.h>
#include <base/sleep.h>
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
    bool streaming_query_,
    LoggerPtr logger_)
    : Streaming::ISource(header_, true, logger_, ProcessorID::FileLogSourceID)
    , file_log(file_log_)
    , query_context(query_context_)
    , head_chunk(header_.getColumns(), 0)
    , max_block_size(max_block_size_)
    , start_timestamp(start_timestamp_)
    , files(std::move(files_))
{
    setStreaming(streaming_query_);

    iter = files.begin();

    initVirtualColumnValueFunctions(header_);

    last_flush_ms = MonotonicMilliseconds::now();
    last_check = last_flush_ms;

    (void)max_block_size;
    (void)start_timestamp;
}

void FileLogSource::initVirtualColumnValueFunctions(const Block & header_)
{
    virtual_col_value_functions.resize(header_.columns());

    for (size_t i = 0; const auto & column : header_)
    {
        if (column.name == "_filename")
        {
            virtual_col_value_functions[i] = [this]() -> String {
                if (current_fd >= 0 && iter != files.end())
                    return iter->second.filename().string();
                else
                    return "";
            };
        }
        else if (column.name == "_filepath")
        {
            virtual_col_value_functions[i] = [this]() -> String {
                if (current_fd >= 0 && iter != files.end())
                    return iter->second.parent_path().string();
                else
                    return "";
            };
        }

        ++i;
    }
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
            auto new_file_fd = ::open(iter->second.c_str(), O_RDONLY);
            if (new_file_fd < 0)
                throwFromErrnoWithPath(
                    "Cannot open file ",
                    iter->second.string(),
                    errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
                    errno);

            setCurrentFile(new_file_fd);

            if (buffer.empty())
                buffer.resize(8 * 1024 * 1024);

            if (!handleCurrentFile())
                return head_chunk.clone();
        }

        return readAndProcess();
    }
    else if (isStreaming())
    {
        checkNewFiles();
    }

    if (isStreaming())
        return head_chunk.clone();
    else
        return {};
}

Chunk FileLogSource::readAndProcess()
{
    assert(current_fd >= 0);

    auto max_to_read = std::min<UInt64>(buffer.size() - buffer_offset, current_file_size - current_offset);
    auto n = ::pread(current_fd, buffer.data() + buffer_offset, max_to_read, current_offset);
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
            /// The last file
            if (isStreaming())
            {
                checkNewFiles();
                return head_chunk.clone();
            }
            else
            {
                return flushBuffer();
            }
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

Chunk FileLogSource::createChunkFor(const std::vector<std::string_view> & lines) const
{
    auto columns = head_chunk.cloneEmptyColumns();
    for (size_t i = 0; const auto & virtual_col_func : virtual_col_value_functions)
    {
        if (!virtual_col_func)
        {
            /// Required physical column
            for (const auto & line : lines)
            {
                if (line.ends_with('\n'))
                    columns[i]->insertData(line.data(), line.size() - 1); /// Remove carriage return to make console visualization easier
                else
                    columns[i]->insertData(line.data(), line.size());
            }
        }
        else
        {
            columns[i]->insertMany(virtual_col_func(), lines.size());
        }

        ++i;
    }

    return Chunk(std::move(columns), lines.size());
}

Chunk FileLogSource::process()
{
    assert(buffer_offset >= remaining);
    auto left = remaining;
    auto lines{breakLines(buffer.data() + (buffer_offset - left), left, file_log->linebreakerRegex(), 512 * 1024)};
    if (!lines.empty())
    {
        auto chunk = createChunkFor(lines);

        /// Move the remaining bytes to the start of buffer
        std::memmove(buffer.data(), buffer.data() + (buffer_offset - left), left);
        remaining = left;
        buffer_offset = left;

        return chunk;
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
        chassert(buffer_offset >= remaining);

        auto chunk = createChunkFor(std::vector{std::string_view{buffer.data() + (buffer_offset - remaining), remaining}});

        remaining = 0;
        buffer_offset = 0;

        return chunk;
    }

    if (isStreaming() || files.size() > 1)
        return head_chunk.clone();
    else
        return {};
}

void FileLogSource::checkNewFiles()
{
    if (MonotonicMilliseconds::now() - last_check <= check_interval_ms)
    {
        sleepForMilliseconds(200);
        return;
    }

    auto new_files{file_log->searchForCandidates()};
    last_check = MonotonicMilliseconds::now();

    if (new_files.empty())
    {
        /// FIXME, notification
        sleepForMilliseconds(200);
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
                LOG_ERROR(logger, "Failed to open log file {}. Skip analyzing it for now", filename);
                continue;
            }

            auto hash_val = calculateFileHash(new_fd, read_buf, read_buf.size(), filename);
            if (file_hashes.contains(hash_val))
            {
                new_iter = new_files.erase(new_iter);
                ::close(new_fd);
                LOG_INFO(logger, "Found handled log file {}. Skip analyzing it", filename);
            }
            else
            {
                ++new_iter;
                file_hashes.emplace(hash_val, filename);

                if (first_new_fd < 0)
                    first_new_fd = new_fd;
                else
                    ::close(new_fd);

                LOG_INFO(logger, "Found new log file {} ", filename);
            }
        }

        if (!new_files.empty())
        {
            LOG_INFO(logger, "Closing current log file {} and progress to {}", iter->second.c_str(), new_files.begin()->second.c_str());

            ::close(current_fd);
            assert(first_new_fd >= 0);
            setCurrentFile(first_new_fd);

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

        LOG_INFO(logger, "Log file was handled, prev_name={} current_name={} hash={}", hash_iter->second, filename, hash_val);
        iter = files.erase(iter);
        return false;
    }

    LOG_INFO(logger, "Processing log file {}, hash={} hashed_bytes={}", filename, hash_val, 1024);

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
            LOG_INFO(logger, "Wait for more data in file={} to calculate its hash", filename);
            sleepForMilliseconds(200);
        }
        else
        {
            if (errno != EINTR)
                DB::throwFromErrnoWithPath("Cannot read from file", filename, DB::ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno);
        }
    }

    return std::hash<std::string_view>{}(std::string_view(read_buf.data(), read_bytes));
}

void FileLogSource::setCurrentFile(int new_file_fd)
{
    current_fd = new_file_fd;
    current_offset = 0;

    if (!isStreaming())
    {
        struct stat stat;
        if (auto ret = ::fstat(current_fd, &stat); ret < 0)
            throwFromErrnoWithPath("Cannot read file size ", iter->second.string(), ErrorCodes::CANNOT_FSTAT, errno);

        current_file_size = stat.st_size;
    }
}
}
