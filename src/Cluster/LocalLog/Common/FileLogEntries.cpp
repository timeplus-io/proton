#include <Cluster/LocalLog/Common/FileLogEntries.h>

#include <base/errnoToString.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int CANNOT_FSTAT;
extern const int CORRUPTED_DATA;
}
}

namespace cluster::nlog
{
FileLogEntriesPtr
FileLogEntries::open(const fs::path & filename, bool file_already_exists, bool read_only, uint64_t initial_file_size, bool preallocate)
{
    auto file = std::make_shared<AppendOnlyFile>(filename, file_already_exists, read_only, initial_file_size, preallocate);
    return std::make_shared<FileLogEntries>(file, 0, std::numeric_limits<uint64_t>::max(), false);
}

FileLogEntries::FileLogEntries(AppendOnlyFilePtr file_, uint64_t start_pos_, uint64_t end_pos_, bool is_slice_)
    : file(file_), start_pos(start_pos_), end_pos(end_pos_), is_slice(is_slice_)
{
    if (auto res = file->size(); res.hasError())
        throw DB::Exception(DB::ErrorCodes::CANNOT_FSTAT, "Failed to get log size, error={}", errnoToString(res.error_code));
    else
        bytes = res.result;

    (void)is_slice;
}

std::shared_ptr<FileLogEntries> FileLogEntries::slice(uint64_t position, uint64_t size_)
{
    assert(size_ != 0);
    auto available_bytes = availableBytes(position, size_);

    return std::make_shared<FileLogEntries>(file, start_pos + position, start_pos + position + available_bytes, true);
}

CallResult<uint64_t> FileLogEntries::read(char * dst, size_t bytes_to_read, uint64_t position)
{
    auto start_offset = start_pos + position;
    return file->read(dst, bytes_to_read, start_offset);
}

size_t FileLogEntries::availableBytes(uint64_t position, uint64_t size_) const
{
    /// Cache the current size in case concurrent write changes it
    uint64_t current_size = bytes;

    assert(start_pos + position <= current_size);

    auto end = start_pos + position + size_;
    if (end > start_pos + current_size || size_ == 0)
        /// Beyond the end of the file
        end = start_pos + current_size;

    return end - start_pos - position;
}

std::optional<FileLogEntries::LogSequencePosition> FileLogEntries::searchForSequenceWithSize(int64_t target_sn, uint64_t starting_position)
{
    uint64_t target_size = 0;
    uint64_t target_position = 0;

    applyLogEntryMetadata(
        [&target_size, &target_position, target_sn](cluster::EntryPtr entry, uint64_t position) -> bool {
            if (entry->sn == target_sn)
            {
                target_size = entry->serializedSize();
                target_position = position;
                return true;
            }
            return false;
        },
        starting_position);

    if (target_size > 0)
        return LogSequencePosition{target_sn, target_position, target_size};

    return {};
}

void FileLogEntries::applyLogEntryMetadata(
    std::function<bool(cluster::EntryPtr, uint64_t)> callback, std::optional<uint64_t> starting_position) const
{
    assert(callback);

    std::vector<char> buf(cluster::Entry::approximateSerializedMetadataSize(), '\0');

    auto start_offset = start_pos;
    if (starting_position.has_value())
        start_offset = starting_position.value();

    auto * data = buf.data();
    int64_t bytes_to_read = buf.size();

    /// Initial read
    while (start_offset < end_pos)
    {
        auto n = file->read(data, bytes_to_read, start_offset);
        if (!n.hasError() && n.result > 0)
        {
            auto entry_meta{cluster::Entry::parse(std::string_view(data, n.result), Nulls::NullVersion, /*metadata_only=*/true)};

            auto next_entry_offset = start_offset + entry_meta->serializedSize();
            if (callback(std::move(entry_meta), start_offset))
                /// Done
                break;
            else
                /// Progress to next log entry's file position
                start_offset = next_entry_offset;
        }
        else
        {
            /// Hit EOF
            break;
        }
    }
}

/// [sn, max_sn)
CallResult<std::pair<uint64_t, cluster::EntryPtrs>>
FileLogEntries::deserialize(int64_t max_sn, size_t max_size, uint64_t max_buffer_size) const
{
    assert(max_sn > 0);

    auto start_offset = start_pos;
    auto end_offset = end_pos;

    /// When FileLogEntries are sliced, it guarantees [start_pos, end_pos) contains complete number of log entries
    /// We don't want to read pass end_pos.
    /// Note: it turns out it is important we need ensure the read range [start_pos, end_pos). If we read passed
    /// the `end_pos`, very weird data corruption will happen. It's like consumer tries to read ahead of producer
    /// or read something producer is producing but not finalizing. GITHUB issue-814
    uint64_t to_read = static_cast<uint64_t>(end_offset - start_offset);
    uint64_t max_to_read = std::min(to_read, max_buffer_size);
    std::vector<char> read_buf(max_to_read);
    /// Initial read
    auto bytes_read = file->read(read_buf.data(), max_to_read, start_offset);
    if (bytes_read.hasError() || bytes_read.result == 0) /// Error or EOF
        return CallResult<std::pair<uint64_t, cluster::EntryPtrs>>{bytes_read.error_code};

    CallResult<std::pair<uint64_t, cluster::EntryPtrs>> log_entries;
    log_entries.result.first = 0;
    log_entries.result.second.reserve(100);

    size_t next_consuming_pos = 0;
    size_t unconsumed = bytes_read.result;
    size_t max_bytes_to_parse = max_size; /// it's advisory
    uint64_t current_file_offset = start_offset + bytes_read.result;

    //  Read More Phase:
    //
    //               current_file_offset        end_offset
    //                        ▲                     ▲
    //                        │                     │
    //                        │                     │
    //        ┌───────────────┼──── ── ── ─── ──────┼─────────────────────────┐
    //        │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│                     │                         │
    //        │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│                     │                         │
    //        └───────────────┼──── ── ── ─── ──────┼─────────────────────────┘
    //        ▲ Partial Entry │                     │
    //        │               │                     │
    //        │               │    Read more data   │
    //        │               │◄── and reach EOF ───►
    //next_consuming_pos      │                     │
    //        │               │                     │
    //        │               │                     │
    //        ◄───unconsumed──►                     │
    //        │               │                     │
    //        │                                     │
    //        │    Concat existing partial entry    │
    //        ◄────    and newly read data     ─────►
    //        │                                     │
    //
    /// Return false if we don't need read more data (we are done), else true
    /// Read log entries until EOF or end_pos
    auto read_more_data = [&](bool resize = false) -> CallResult<bool> {
        if (likely(!resize))
        {
            /// First copy unconsumed to the beginning of the read buffer if necessary
            /// We only move the data to the beginning of the read buffer when we reach
            /// the end of the read buffer (no available room at the end of the buffer)
            /// Sometimes, file system has short read (read n bytes but return m bytes)
            if (likely(read_buf.size() == next_consuming_pos + unconsumed))
            {
                std::memmove(read_buf.data(), read_buf.data() + next_consuming_pos, read_buf.size() - next_consuming_pos);
                next_consuming_pos = 0;
            }
        }
        else
            /// We encounter too big single log entry. TODO, throw since likely malformed ?
            read_buf.resize(read_buf.size() * 2);

        /// Read more data. We don't want read passed end_pos
        max_to_read = std::min<uint64_t>(end_offset - current_file_offset + 1, read_buf.size() - next_consuming_pos - unconsumed);

        auto r = file->read(read_buf.data() + next_consuming_pos + unconsumed, max_to_read, current_file_offset);
        if (r.hasError() || r.result == 0) /// Error or EOF
            return CallResult<bool>{false, r.error_code};

        unconsumed += r.result;
        current_file_offset += r.result;

        return CallResult<bool>{true, DB::ErrorCodes::OK};
    };

    // Parsing Phase:
    //
    // start_offset                                               current_file_offset        end_offset
    //     ▲                                                               ▲                     ▲
    //     │                                                               │                     │
    //     ├─────────────────────── 16MB Read Buffer  ─────────────────────┤                     │
    //     │                                                               │                     │
    //     ◄────────────── complete parsed entries ───────►┐               │                     │
    //     │                                               │               │                     │
    //     ├────────┬─────────────┬───── ── ─── ───────────┼───────────────┼──── ── ── ── ───────┤
    //     │        │             │                        │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│                     │
    //     │        │             │                        │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│                     │
    //     └────────┴─────────────┴───── ── ─── ───────────┴───────────────┼──── ── ── ── ───────┘
    //      Entry       Entry            Entries           ▲ Partial Entry │
    //                                                     │               │
    //                                                     │               │
    //                                             next_consuming_pos      │
    //                                                     │               │
    //                                                     │               │
    //                                                     ◄───unconsumed──►
    //                                                     │               │
    //
    while (true)
    {
        auto * data = read_buf.data();

        auto parsed_bytes = cluster::Entry::parseMany(
            std::string_view(data + next_consuming_pos, unconsumed - next_consuming_pos),
            Nulls::NullVersion,
            max_sn,
            max_bytes_to_parse,
            log_entries.result.second);
        if (parsed_bytes.hasError())
        {
            log_entries.result.second.clear();
            log_entries.error_code = parsed_bytes.error_code;
            break;
        }

        /// Accumulate the parsed bytes
        log_entries.result.first += parsed_bytes.result;

        /// Don't parse more if exceed \max_size
        if (max_bytes_to_parse > parsed_bytes.result)
            max_bytes_to_parse -= parsed_bytes.result;
        else
            break;

        if (!log_entries.result.second.empty() && log_entries.result.second.back()->sn + 1 >= max_sn)
        {
            /// We are done with parsing
            assert(log_entries.result.second.back()->sn < max_sn);
            break;
        }

        if (current_file_offset >= end_offset)
            break;

        next_consuming_pos += parsed_bytes.result;

        assert(unconsumed >= next_consuming_pos);
        unconsumed -= next_consuming_pos;

        auto need_read_more_data = read_more_data(unconsumed >= read_buf.size());
        if (need_read_more_data.hasError() || !need_read_more_data.result) /// Either we have error or we are done with reading
        {
            /// Setup the error code and we still like to return what we have read since they may be still useful
            log_entries.error_code = need_read_more_data.error_code;
            break;
        }
    }

    return log_entries;
}
}
