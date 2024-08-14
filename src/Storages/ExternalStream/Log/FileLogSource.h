#pragma once

#include <Processors/Streaming/ISource.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <filesystem>

namespace Poco
{
class Logger;
}

namespace DB
{
struct ExternalStreamSettings;

class FileLog;

class FileLogSource final : public Streaming::ISource
{
public:
    using FileContainer = std::set<std::pair<Int64, std::filesystem::path>>;

    FileLogSource(
        FileLog * file_log_,
        Block header_,
        ContextPtr query_context_,
        size_t max_block_size,
        Int64 start_timestamp_,
        FileContainer files_,
        Poco::Logger * log_);

    ~FileLogSource() override;

    String getName() const override { return "LogStream"; }

    Chunk generate() override;

private:
    bool handleCurrentFile();

    size_t calculateFileHash(int fd, std::vector<char> & read_buf, size_t bytes_to_read, const String & filename);

    Chunk readAndProcess();
    Chunk process();
    Chunk flushBuffer();
    void checkNewFiles();

private:
    FileLog * file_log;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;
    DataTypePtr column_type;
    Chunk head_chunk;
    size_t max_block_size;

    Int64 start_timestamp;

    FileContainer files;
    FileContainer::iterator iter;
    int current_fd = -1;
    Int64 current_offset = -1;

    /// Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;

    Int64 check_interval_ms = 30000;
    Int64 last_check = 0;

    std::vector<char> buffer;
    size_t buffer_offset = 0;
    size_t remaining = 0;

    std::unordered_map<size_t, std::string> file_hashes;

    Poco::Logger * log;
};
}
