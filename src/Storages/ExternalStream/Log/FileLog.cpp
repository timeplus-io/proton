#include <Interpreters/Context.h>
#include <NativeLog/Base/Stds.h>
#include <NativeLog/Record/Record.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Log/FileLogSource.h>
#include <Storages/ExternalStream/Log/fileLastModifiedTime.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/Streaming/storageUtil.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string.hpp>
#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int CANNOT_FSTAT;
}

FileLog::FileLog(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context)
    : StorageExternalStreamImpl(storage, std::move(settings_), context)
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
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    Block header;

    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
    {
        auto physical_columns{storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary))};
        const auto & any_one_column = physical_columns.front();
        header.insert({any_one_column.type->createColumn(), any_one_column.type, any_one_column.name});
    }

    assert(query_info.seek_to_info);
    auto saved_start_timestamp = query_info.seek_to_info->getSeekPoints()[0];

    bool table_query = context->getSettings().query_mode.value == "table";

    /// SeekToInfo parsed with UTC timezone, we should re-parse with local timezone.
    /// FIXME better implementation
    if (query_info.seek_to_info->getSeekToType() == SeekToType::RELATIVE_TIME)
        saved_start_timestamp = SeekToInfo::parse(query_info.seek_to_info->getSeekTo(), false).second[0];

    return Pipe(std::make_shared<FileLogSource>(
        this, std::move(header), std::move(context), max_block_size, saved_start_timestamp, searchForCandidates(table_query), log));
}

FileLogSource::FileContainer FileLog::searchForCandidates(bool table_query)
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
                if (table_query)
                {
                    /// Historic query: use all files to query
                    candidates.emplace(last_modified, std::filesystem::canonical(dir_entry.path()));
                }
                else if (last_modified >= start_timestamp)
                    /// Streaming query: use the latest file to tail
                    candidates.emplace(last_modified, std::filesystem::canonical(dir_entry.path()));

                break;
            }
        }
    }

    if (table_query && start_timestamp == nlog::LATEST_SN)
    {
        /// Tail the log last file
        /// Remove all other files except the last one
        for (; !candidates.empty() && candidates.size() != 1;)
            candidates.erase(candidates.begin());
    }

    LOG_INFO(
        log,
        "Using {} regexes: {} to search log_dir={} with start_timestamp={}, found {} candidates",
        file_regexes.size(),
        settings->log_files.value,
        settings->log_dir.value,
        start_timestamp,
        candidates.size());

    if (!candidates.empty())
        /// Update start timestamp for next scan
        start_timestamp = candidates.rbegin()->first;

    return candidates;
}
}
