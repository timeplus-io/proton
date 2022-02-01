#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace nlog
{
struct LogConfig
{
    static const uint64_t DEFAULT_MAX_MESSAGE_SIZE = 10 * 1024 * 1024;
    static const uint64_t DEFAULT_SEGMENT_SIZE =  4ull * 1024 * 1024 * 1024;
    static const int64_t DEFAULT_SEGMENT_MS = 24ull * 7 * 60 * 60 * 1000;
    static const int64_t DEFAULT_FLUSH_INTERVAL =  1000 * 1000;
    static const int64_t DEFAULT_FLUSH_MS = 10 * 60 * 1000;
    static const int64_t DEFAULT_RETENTION_BYTES = -1;
    static const int64_t DEFAULT_RETENTION_MS = 24ull * 7 * 60 * 60 * 1000;
    static const int64_t DEFAULT_LOG_DELETE_DELAY_MS =  60 * 1000;
    static const uint64_t DEFAULT_INDEX_INTERVAL_BYTES = 4096;
    static const uint64_t DEFAULT_INDEX_INTERVAL_COUNT = 100;
    static const uint64_t DEFAULT_MAX_INDEX_BYTES = 10 * 1024 * 1024;

    uint64_t max_message_size = DEFAULT_MAX_MESSAGE_SIZE;
    uint64_t segment_size = DEFAULT_SEGMENT_SIZE;
    int64_t segment_ms = DEFAULT_SEGMENT_MS;
    /// per X messages
    int64_t flush_interval = DEFAULT_FLUSH_INTERVAL;
    /// per X milliseconds
    int64_t flush_ms = DEFAULT_FLUSH_MS;
    int64_t retention_size = DEFAULT_RETENTION_BYTES;
    int64_t retention_ms = DEFAULT_RETENTION_MS;

    int64_t file_delete_delay_ms = DEFAULT_LOG_DELETE_DELAY_MS;
    uint32_t max_index_size = DEFAULT_MAX_INDEX_BYTES;
    uint64_t index_interval_bytes = DEFAULT_INDEX_INTERVAL_BYTES;
    uint64_t index_interval_count = DEFAULT_INDEX_INTERVAL_COUNT;
    bool preallocate = true;

    bool compact() const
    {
        /// FIXME
        return false;
    }

    int32_t initFileSize() const
    {
        if (preallocate)
            return segment_size;
        else
            return 0;
    }

    std::string string() const
    {
        /// FIXME
        return "";
    }
};

using LogConfigPtr = std::shared_ptr<LogConfig>;
}
