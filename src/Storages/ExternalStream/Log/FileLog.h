#pragma once

#include "FileLogSource.h"

#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#include <filesystem>

namespace re2
{
class RE2;
}

namespace Poco
{
class Logger;
}

namespace DB
{

class IStorage;

class FileLog final : public StorageExternalStreamImpl
{
public:
    FileLog(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_);
    ~FileLog() override = default;

    void startup() override { }
    void shutdown() override { }

    UInt64 hashBytes() const { return std::min<UInt64>(std::max<UInt64>(128, settings->hash_bytes.value), 2048); }

    const re2::RE2 & timestampRegex() const { return *timestamp_regex; }

    const re2::RE2 & linebreakerRegex() const { return *linebreaker_regex; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    FileLogSource::FileContainer searchForCandidates();

private:
    StorageID storage_id;
    std::unique_ptr<ExternalStreamSettings> settings;
    std::vector<std::unique_ptr<re2::RE2>> file_regexes;
    std::unique_ptr<re2::RE2> timestamp_regex;
    std::unique_ptr<re2::RE2> linebreaker_regex;

    Int64 start_timestamp;

    Poco::Logger * log;
};
}
