#include "FileRecords.h"

#include <base/defines.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace nlog
{
FileRecordsPtr FileRecords::open(const fs::path & filename, bool file_already_exists, bool read_only)
{
    auto file = std::make_shared<AppendOnlyFile>(filename, file_already_exists, read_only);
    return std::make_shared<FileRecords>(file, 0, std::numeric_limits<uint64_t>::max(), false);
}

FileRecords::FileRecords(AppendOnlyFilePtr file_, uint64_t start_pos_, uint64_t end_pos_, bool is_slice_)
    : file(file_), start_pos(start_pos_), end_pos(end_pos_), is_slice(is_slice_), bytes(file->size())
{
    (void)is_slice;
    (void)end_pos;
}

int64_t FileRecords::append(const MemoryRecords & records)
{
    auto data = records.data();
    size_t to_write = data.size();

    size_t written = 0;
    while (written < to_write)
    {
        auto n = file->append(data.data() + written, to_write - written);
        if (n < 0)
            /// Interrupted
            continue;

        written += n;
    }

    bytes += written;

    return written;
}

std::shared_ptr<FileRecords> FileRecords::slice(uint32_t position, uint32_t size_)
{
    auto available_bytes = availableBytes(position, size_);

    return std::make_shared<FileRecords>(file, start_pos + position, start_pos + position + available_bytes, true);
}

int64_t FileRecords::read(uint8_t * dst, size_t count, uint32_t position)
{
    auto start_offset = start_pos + position;
    size_t read_bytes = 0;
    while (read_bytes < count)
    {
        auto n = file->read(dst, count, start_offset + read_bytes);
        if (n < 0)
            // Interrupted
            continue;
        else if (n == 0)
            /// EOF
            break;

        read_bytes += n;
    }
    return read_bytes;
}

size_t FileRecords::availableBytes(uint32_t position, uint32_t size_) const
{
    /// Cache the current size in case concurrent write changes it
    size_t current_size = bytes;

    if (start_pos + position > current_size)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Slice from position {} exceeds end position of {}", position, current_size);

    auto end = start_pos + position + size_;
    if (end > start_pos + current_size)
        /// Beyond the end of the file
        end = start_pos + current_size;

    return end - start_pos - position;
}

FileRecords::LogOffsetPosition FileRecords::searchForOffsetWithSize(int64_t target_offset, uint64_t starting_position)
{
    uint64_t target_size = 0;
    uint64_t target_position = 0;

    apply(
        [&target_size, &target_position, target_offset](const MemoryRecords & batch, uint64_t position) -> bool {
            auto offset = batch.lastOffset();
            if (offset >= target_offset)
            {
                target_size = batch.sizeInBytes();
                target_position = position;
                return true;
            }
            return false;
        },
        starting_position);

    if (target_size > 0)
        return {target_offset, target_position, target_size};

    return {-1, 0, 0};
}

void FileRecords::apply(std::function<bool(const MemoryRecords &, uint64_t)> callback, std::optional<uint64_t> starting_position, size_t bufsize)
{
    std::vector<uint8_t> buf(std::min<size_t>(8192 * 1024, bufsize), '\0');

    auto start_offset = start_pos;
    if (starting_position.has_value())
        start_offset = starting_position.value();

    /// Initial read
    auto n = file->read(buf.data(), buf.size(), start_offset);
    if (n <= 0)
        /// EOF or Error
        return;

    size_t next_consuming_pos = 0;
    size_t unconsumed = n;
    uint64_t current_file_position = n + start_offset;

    /// Return true done, else false
    auto read_data = [&](bool resize = false) -> bool {
        if (likely(!resize))
        {
            /// First copy unconsumed to the beginning of the buf if necessary
            if (buf.size() == next_consuming_pos + unconsumed)
            {
                for (auto start = next_consuming_pos, index = 0ul; start < buf.size(); ++start, ++index)
                    buf[index] = buf[start];

                next_consuming_pos = 0;
            }
        }
        else
            buf.resize(buf.size() * 2);

        /// Read more data
        auto r
            = file->read(buf.data() + next_consuming_pos + unconsumed, buf.size() - next_consuming_pos - unconsumed, current_file_position);
        if (r <= 0)
            return true;

        unconsumed += r;
        current_file_position += r;

        return false;
    };

    auto constexpr prefix_len_size = sizeof(flatbuffers::uoffset_t);

    /// Read RecordBatch until EOF or done with reading from callback
    while (1)
    {
        if (unlikely(unconsumed < prefix_len_size))
        {
            if (read_data())
                break;
            else
                continue;
        }

        auto batch_size = flatbuffers::ReadScalar<flatbuffers::uoffset_t>(buf.data() + next_consuming_pos) + prefix_len_size;
        if (batch_size > unconsumed)
        {
            /// We don't have enough data for this batch, read more
            if (read_data(unconsumed >= buf.size()))
                break;
            else
                continue;
        }

        MemoryRecords records(std::span<uint8_t>(buf.data() + next_consuming_pos, batch_size));

        unconsumed -= batch_size;
        next_consuming_pos += batch_size;
        ///                                                      current_file_position
        ///                                                                |
        ///                                                                v
        ///  |---record_batch--with_batch_size---|---read_but_unconsumed---|
        ///  ^
        ///  |
        ///  batch_start_file_position
        auto batch_start_file_position = current_file_position - unconsumed - batch_size;

        if (callback(records, batch_start_file_position))
            /// Done
            break;

        if (current_file_position - unconsumed >= end_pos)
            /// We reach the maximum file position of the FileRecords slice
            break;
    }
}
}
