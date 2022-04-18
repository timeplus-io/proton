#include "FileLog.h"
#include "FileLogSource.h"
#include "fileLastModifiedTime.h"

#include <Interpreters/Context.h>
#include <NativeLog/Base/Stds.h>
#include <NativeLog/Record/Record.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/IStorage.h>
#include <Storages/Streaming/parseSeekTo.h>

#include <boost/algorithm/string.hpp>
#include <re2/re2.h>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int CANNOT_FSTAT;
}

FileLog::FileLog(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_)
    : storage_id(storage->getStorageID())
    , settings(std::move(settings_))
    , timestamp_regex(std::make_unique<re2::RE2>(settings->timestamp_regex.value))
    , linebreaker_regex(std::make_unique<re2::RE2>(settings->row_delimiter.value))
    , log(&Poco::Logger::get("External-FileLog"))
{
    assert(settings->type.value == StreamTypes::LOG);

    if (settings->log_files.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `log_files` setting for {} external stream", settings->type.value);

    if (settings->log_dir.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `log_dir` setting for {} external stream", settings->type.value);


    if (settings->timestamp_regex.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `timestamp_regex` setting for {} external stream", settings->type.value);

    if (!timestamp_regex->ok())
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid `timestamp_regex` setting for {} external stream, error={}",
            settings->type.value,
            timestamp_regex->error());

    if (settings->row_delimiter.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `row_delimiter` setting for {} external stream", settings->type.value);

    if (!linebreaker_regex->ok())
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid regex `row_delimiter` setting for {} external stream, error={}",
            settings->type.value,
            linebreaker_regex->error());

    if (linebreaker_regex->NumberOfCapturingGroups() != 1)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "regex `row_delimiter` requires only 1 capturing group for {} external stream, got capture groups={}",
            settings->type.value,
            linebreaker_regex->NumberOfCapturingGroups());

    std::vector<String> file_paths;
    boost::split(file_paths, settings->log_files.value, boost::is_any_of(","));

    for (auto & file_path : file_paths)
    {
        if (!file_path.empty())
        {
            file_regexes.push_back(std::make_unique<re2::RE2>(file_path));
            if (!file_regexes.back()->ok())
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "regex `log_files` is invalid for {} external stream, error={}",
                    settings->type.value,
                    file_regexes.back()->error());
        }
    }
}

Pipe FileLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    start_timestamp = parseSeekTo(context->getSettingsRef().seek_to.value, false);

    Block header;

    if (!column_names.empty())
        header = metadata_snapshot->getSampleBlockForColumns(column_names, {}, storage_id);
    else
    {
        auto physical_header{metadata_snapshot->getSampleBlockNonMaterialized()};
        header.insert(physical_header.getByPosition(0));
    }

    auto saved_start_timestamp = start_timestamp;

    return Pipe(std::make_shared<FileLogSource>(
        this,
        std::move(header),
        std::move(context),
        max_block_size,
        saved_start_timestamp,
        searchForCandidates(),
        log));
}

FileLogSource::FileContainer FileLog::searchForCandidates()
{
    FileLogSource::FileContainer candidates;

    /// Initially scan the file system to figure out the target log files.
    /// Sort them by last modified timestamps (in future, sort the files by last timestamp in the logs)
    for (const auto & dir_entry : std::filesystem::directory_iterator{settings->log_dir.value})
    {
        if (dir_entry.is_directory() || !dir_entry.is_regular_file())
            continue;

        for (const auto & file_regex : file_regexes)
        {
            auto filename{dir_entry.path().filename().string()};
            if (file_regex->Match(filename, 0, filename.size(), re2::RE2::ANCHOR_BOTH, nullptr, 0))
            {
                auto last_modified = lastModifiedTime(dir_entry.path());
                if (last_modified >= start_timestamp)
                    candidates.emplace(last_modified, std::filesystem::canonical(dir_entry.path()));

                break;
            }
        }
    }

    if (start_timestamp == nlog::LATEST_SN)
    {
        /// Tail the log last file
        /// Remove all other files except the last one
        for (; !candidates.empty() && candidates.size() != 1;)
            candidates.erase(candidates.begin());
    }

    if (!candidates.empty())
        /// Update start timestamp for next scan
        start_timestamp = candidates.rbegin()->first;

    return candidates;
}
}
