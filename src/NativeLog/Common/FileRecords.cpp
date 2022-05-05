#include "FileRecords.h"

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
}

int64_t FileRecords::append(const ByteVector & record)
{
    auto written = file->append(record.data(), record.size());
    bytes += written;
    return written;
}

std::shared_ptr<FileRecords> FileRecords::slice(uint64_t position, uint64_t size_)
{
    assert(size_ != 0);
    auto available_bytes = availableBytes(position, size_);

    return std::make_shared<FileRecords>(file, start_pos + position, start_pos + position + available_bytes, true);
}

int64_t FileRecords::read(char * dst, size_t bytes_to_read, uint64_t position)
{
    auto start_offset = start_pos + position;
    return file->read(dst, bytes_to_read, start_offset);
}

size_t FileRecords::availableBytes(uint64_t position, uint64_t size_) const
{
    /// Cache the current size in case concurrent write changes it
    uint64_t current_size = bytes;

    if (start_pos + position > current_size)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Slice from position {} exceeds end position of {}", position, current_size);

    auto end = start_pos + position + size_;
    if (end > start_pos + current_size || size_ == 0)
        /// Beyond the end of the file
        end = start_pos + current_size;

    return end - start_pos - position;
}

FileRecords::LogSequencePosition FileRecords::searchForSequenceWithSize(int64_t target_sequence, uint64_t starting_position)
{
    uint64_t target_size = 0;
    uint64_t target_position = 0;

    applyRecordMetadata(
        [&target_size, &target_position, target_sequence](RecordPtr record, uint64_t position) -> bool {
            if (record->getSN() == target_sequence)
            {
                target_size = record->serializedBytes();
                target_position = position;
                return true;
            }
            return false;
        },
        starting_position);

    if (target_size > 0)
        return {target_sequence, target_position, target_size};

    return {-1, 0, 0};
}

void FileRecords::applyRecordMetadata(std::function<bool(RecordPtr, uint64_t)> callback, std::optional<uint64_t> starting_position) const
{
    assert(callback);

    std::vector<char> buf(Record::commonMetadataBytes() + Record::prefixLengthSize(), '\0');

    auto start_offset = start_pos;
    if (starting_position.has_value())
        start_offset = starting_position.value();

    auto * data = buf.data();
    int64_t bytes_to_read = buf.size();

    /// Initial read
    while (1)
    {
        auto n = file->read(data, bytes_to_read, start_offset);
        if (n == bytes_to_read)
        {
            auto record{Record::deserializeCommonMetadata(data, n)};

            auto next_record_offset = start_offset + record->totalSerializedBytes();
            if (callback(std::move(record), start_offset))
                /// done
                break;
            else
                /// progress to next record's file position
                start_offset = next_record_offset;
        }
        else
            /// hit EOF
            break;
    }
}

RecordPtrs FileRecords::deserialize(std::vector<char> & read_buf, const SchemaContext & schema_ctx) const
{
    auto start_offset = start_pos;
    auto end_offset = end_pos;

    /// When FileRecords are sliced, it guarantees [start_pos, end_pos) contains complete number of records
    /// We don't want to read pass end_pos.
    /// Note: it turns out it is important we need ensure the read range [start_pos, end_pos). If we read passed
    /// the `end_pos`, very weird data corruption will happen. It's like consumer tries to read ahead of producer
    /// or read something producer is producing but not finalizing. GITHUB issue-814
    auto max_to_read = std::min<uint64_t>(end_offset - start_offset, read_buf.size());

    /// Initial read
    auto n = file->read(read_buf.data(), max_to_read, start_offset);
    if (n <= 0)
        /// EOF or Error
        return {};

    RecordPtrs records;
    records.reserve(100);

    size_t next_consuming_pos = 0;
    size_t unconsumed = n;
    uint64_t current_file_offset = n + start_offset;

    /// Return true done, else false
    auto read_more_data = [&](bool resize = false) -> bool {
        if (likely(!resize))
        {
            /// First copy unconsumed to the beginning of the buf if necessary
            if (read_buf.size() == next_consuming_pos + unconsumed)
            {
                ::memmove(read_buf.data(), read_buf.data() + next_consuming_pos, read_buf.size() - next_consuming_pos);
                next_consuming_pos = 0;
            }
        }
        else
            read_buf.resize(read_buf.size() * 2);

        /// Read more data. We don't want read passed end_pos
        max_to_read = std::min<uint64_t>(end_offset - current_file_offset + 1, read_buf.size() - next_consuming_pos - unconsumed);

        auto r = file->read(read_buf.data() + next_consuming_pos + unconsumed, max_to_read, current_file_offset);
        if (r <= 0)
            return true;

        unconsumed += r;
        current_file_offset += r;

        return false;
    };

    /// Read Record until EOF or end_pos
    while (1)
    {
        next_consuming_pos = Record::deserialize(read_buf.data() + next_consuming_pos, unconsumed, records, schema_ctx);
        assert(unconsumed >= next_consuming_pos);
        unconsumed -= next_consuming_pos;

        if (current_file_offset >= end_offset)
            break;

        if (read_more_data(unconsumed >= read_buf.size()))
            break;
    }

    return records;
}

}
