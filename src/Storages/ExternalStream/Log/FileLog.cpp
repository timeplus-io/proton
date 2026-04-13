#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Log/FileLogSource.h>
#include <Storages/ExternalStream/Log/fileLastModifiedTime.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterUtil.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/Stream/storageUtil.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string.hpp>
#include <Common/re2.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int CANNOT_FSTAT;
}

FileLog::FileLog(
    StorageID storage_id, StorageInMemoryMetadata metadata, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context)
    : StorageExternalStreamImpl(std::move(storage_id), std::move(metadata), std::move(settings_), context)
    , timestamp_regex(std::make_unique<re2::RE2>(settings->timestamp_regex.value))
    , linebreaker_regex(std::make_unique<re2::RE2>(settings->row_delimiter.value))
{
    chassert(settings->type.value == StreamTypes::LOG);

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

QueryProcessingStage::Enum FileLog::getQueryProcessingStage(
    ContextPtr /*local_context*/,
    QueryProcessingStage::Enum /*to_stage*/,
    const StorageSnapshotPtr &,
    SelectQueryInfo & /*query_info*/) const
{
    return QueryProcessingStage::FetchColumns;
}

void FileLog::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto fetch_columns_header = [&]() {
        /// If it is a plain select to fetch columns, then use the required column names as the header
        if (!column_names.empty())
            return storage_snapshot->getSampleBlockForColumns(column_names);
        else
            return storage_snapshot->getSampleBlockForColumns({preferredColumn().value()});
    };

    auto cluster = query_info.getCluster();
    if (cluster != nullptr)
    {
        /// Distributed Query
        auto modified_query_info = query_info;
        auto storage_id = getStorageID();

        auto update_remote_query_and_calc_output_header = [&]() {
            if (query_info.isStreaming())
                return fetch_columns_header();
            else
                /// Distributed historical query
                return InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).noModify().analyze())
                    .getSampleBlock();
        };

        auto header = update_remote_query_and_calc_output_header();
        ClusterProxy::SelectStreamFactory select_stream_factory
            = ClusterProxy::SelectStreamFactory(header, /*objects_by_shard_=*/{}, storage_snapshot, processed_stage);

        ClusterProxy::executeQueryWithParallelReplicas(
            query_plan,
            storage_id,
            /*table_func_ptr=*/nullptr,
            select_stream_factory,
            modified_query_info.query,
            context,
            modified_query_info,
            modified_query_info.getCluster(),
            modified_query_info.isStreaming(),
            /*parallelized_to_all_replicas=*/true);
    }
    else
    {
        /// Local plan
        chassert(query_info.seek_to_info);

        auto header = fetch_columns_header();
        auto saved_start_timestamp = query_info.seek_to_info->getSeekPoints()[0];
        bool streaming_query = query_info.isStreaming();

        /// SeekToInfo parsed with UTC timezone, we should re-parse with local timezone.
        /// FIXME better implementation
        if (query_info.seek_to_info->getSeekToType() == SeekToType::RELATIVE_TIME)
            saved_start_timestamp = SeekToInfo::parse(query_info.seek_to_info->getSeekTo(), false).second[0];

        auto candidate_files = searchForCandidates(streaming_query);

        Pipe pipe;
        if (streaming_query || num_streams == 1 || candidate_files.size() <= 1)
        {
            pipe = Pipe(std::make_shared<FileLogSource>(
                this,
                std::move(header),
                context,
                max_block_size,
                saved_start_timestamp,
                std::move(candidate_files),
                streaming_query,
                logger));
        }
        else
        {
            /// Parallelization by dividing files among num_streams
            auto num_files = candidate_files.size();
            size_t shares = 0;
            if (num_files % num_streams == 0)
                shares = num_files / num_streams;
            else
                shares = num_files / num_streams + 1;

            Pipes pipes;
            pipes.reserve(num_streams);

            std::vector<std::pair<Int64, std::filesystem::path>> candidates{candidate_files.begin(), candidate_files.end()};

            size_t i = 0;
            for (; i < num_files;)
            {
                auto j = i + shares;
                if (j > num_files)
                    j = num_files;

                pipes.emplace_back(std::make_shared<FileLogSource>(
                    this,
                    header,
                    context,
                    max_block_size,
                    saved_start_timestamp,
                    std::set<std::pair<Int64, std::filesystem::path>>{candidates.begin() + i, candidates.begin() + j},
                    streaming_query,
                    logger));

                i = j;
            }

            pipe = Pipe::unitePipes(std::move(pipes));
        }

        /// Override the maximum concurrency
        auto max_parallel_streams = std::max<size_t>(pipe.maxParallelStreams(), context->getSettingsRef().min_threads.value);
        query_plan.setMaxThreads(max_parallel_streams);

        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName(), query_info.storage_limits);
        query_plan.addStep(std::move(read_step));
    }
}

FileLogSource::FileContainer FileLog::searchForCandidates(bool streaming_query)
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
                if (streaming_query && last_modified >= start_timestamp)
                    /// Streaming query: use the latest file to tail
                    candidates.emplace(last_modified, std::filesystem::canonical(dir_entry.path()));
                else
                    /// Historic query: use all files to query
                    candidates.emplace(last_modified, std::filesystem::canonical(dir_entry.path()));

                break;
            }
        }
    }

    if (streaming_query && start_timestamp == cluster::Constants::LatestSN)
    {
        /// Tail the log last file
        /// Remove all other files except the last one
        for (; !candidates.empty() && candidates.size() != 1;)
            candidates.erase(candidates.begin());
    }

    LOG_INFO(
        logger,
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

bool FileLog::isRemote() const
{
    return false;
}

NamesAndTypesList FileLog::getVirtuals() const
{
    return NamesAndTypesList{
        {"_filename", std::make_shared<DataTypeString>()},
        {"_filepath", std::make_shared<DataTypeString>()},
    };
}

std::optional<String> FileLog::preferredColumn() const
{
    return "_filename";
}

}
