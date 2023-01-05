#pragma once

#include <Compression/CompressionInfo.h>

#include <cstdint>
#include <memory>
#include <string>

#include <fmt/format.h>

namespace nlog
{
struct LogConfig
{
    static const int64_t DEFAULT_MAX_RECORD_SIZE = 10 * 1024 * 1024;
    static const int64_t DEFAULT_SEGMENT_SIZE = 1ull * 1024 * 1024 * 1024;
    static const int64_t DEFAULT_SEGMENT_MS = 24ull * 7 * 60 * 60 * 1000;
    static const int64_t DEFAULT_FLUSH_INTERVAL_RECORDS = 1000;
    static const int64_t DEFAULT_FLUSH_MS = 2 * 60 * 1000;
    static const int64_t DEFAULT_RETENTION_BYTES = -1;
    static const int64_t DEFAULT_RETENTION_MS = 24ull * 7 * 60 * 60 * 1000;
    static const int64_t DEFAULT_LOG_DELETE_DELAY_MS = 300 * 1000;
    static const int64_t DEFAULT_INDEX_INTERVAL_BYTES = 4096;
    static const int64_t DEFAULT_INDEX_INTERVAL_RECORDS = 1000;
    static const int64_t DEFAULT_MAX_INDEX_BYTES = 10 * 1024 * 1024;

    int64_t max_record_size = DEFAULT_MAX_RECORD_SIZE;
    int64_t segment_size = DEFAULT_SEGMENT_SIZE;
    int64_t segment_ms = DEFAULT_SEGMENT_MS;
    /// per X messages
    int64_t flush_interval_records = DEFAULT_FLUSH_INTERVAL_RECORDS;
    /// per X milliseconds
    int64_t flush_interval_ms = DEFAULT_FLUSH_MS;
    int64_t retention_size = DEFAULT_RETENTION_BYTES;
    int64_t retention_ms = DEFAULT_RETENTION_MS;

    int64_t file_delete_delay_ms = DEFAULT_LOG_DELETE_DELAY_MS;
    int32_t max_index_size = DEFAULT_MAX_INDEX_BYTES;
    int64_t index_interval_bytes = DEFAULT_INDEX_INTERVAL_BYTES;
    int64_t index_interval_records = DEFAULT_INDEX_INTERVAL_RECORDS;
    DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE;
    bool preallocate = true;

    bool compact() const
    {
        /// FIXME
        return false;
    }

    int32_t initFileSize() const
    {
        if (preallocate)
            return static_cast<int32_t>(segment_size);
        else
            return 0;
    }

    std::shared_ptr<LogConfig> clone() const
    {
        return std::make_shared<LogConfig>(*this);
    }

    std::string string() const
    {
        return fmt::format(
            "max_record_size={} segment_size={} segment_ms={} flush_interval_records={} flush_interval_ms={} retention_size={} "
            "retention_ms={} file_delete_delay_ms={} max_index_size={} index_interval_bytes={} index_interval_records={} preallocate={}",
            max_record_size,
            segment_size,
            segment_ms,
            flush_interval_records,
            flush_interval_ms,
            retention_size,
            retention_ms,
            file_delete_delay_ms,
            max_index_size,
            index_interval_bytes,
            index_interval_records,
            preallocate);
    }
};

using LogConfigPtr = std::shared_ptr<LogConfig>;
}
