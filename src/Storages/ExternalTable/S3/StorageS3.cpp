#include <Storages/ExternalTable/S3/StorageS3.h>

#if USE_AWS_S3

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/IOThreadPool.h>
#include <IO/ParallelReadBuffer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/isValidUTF8.h>

#include <IO/S3/Requests.h>
#include <IO/S3Common.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ExpressionListParsers.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/PartitionedSink.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageURL.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/getVirtualsForStorage.h>
#include <Common/NamedCollections/NamedCollections.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/StoredObject.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteBufferWithNextCallback.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/narrowPipe.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>

#include <re2/re2.h>
#include <Common/CurrentMetrics.h>
#include <Common/parseGlobs.h>
#include <Common/quoteString.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric StorageS3Threads;
extern const Metric StorageS3ThreadsActive;
}

namespace ProfileEvents
{
extern const Event S3DeleteObjects;
extern const Event S3ListObjects;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PARSE_TEXT;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int S3_ERROR;
extern const int UNEXPECTED_EXPRESSION;
extern const int DATABASE_ACCESS_DENIED;
extern const int CANNOT_EXTRACT_STREAM_STRUCTURE;
extern const int NOT_IMPLEMENTED;
extern const int INVALID_SETTING_VALUE;
}

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

namespace
{

const String PARTITION_ID_WILDCARD = "{_partition_id}";

const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};
const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "access_key_id",
    "secret_access_key",
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
};

void addPathToVirtualColumns(Block & block, const String & path, size_t idx)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        assert(pos != std::string::npos);

        auto file = path.substr(pos + 1);
        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

}

namespace ExternalTable
{

class StorageS3Source::DisclosedGlobIterator::Impl : WithContext
{
public:
    Impl(
        const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
        const DB::S3::URI & globbed_uri_,
        ASTPtr & query_,
        const Block & virtual_header_,
        ContextPtr context_,
        KeysWithInfo * read_keys_,
        const S3Settings::RequestSettings & request_settings_,
        std::function<void(FileProgress)> file_progress_callback_)
        : WithContext(context_)
        , client(client_)
        , globbed_uri(globbed_uri_)
        , query(query_)
        , virtual_header(virtual_header_)
        , read_keys(read_keys_)
        , request_settings(request_settings_)
        , list_objects_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, 1)
        , list_objects_scheduler(threadPoolCallbackRunner<ListObjectsOutcome>(list_objects_pool, "ListObjects"))
        , file_progress_callback(file_progress_callback_)
    {
        if (globbed_uri.bucket.find_first_of("*?{") != globbed_uri.bucket.npos)
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Expression can not have wildcards inside bucket name");

        const String key_prefix = globbed_uri.key.substr(0, globbed_uri.key.find_first_of("*?{"));

        /// We don't have to list bucket, because there is no asterisks.
        if (key_prefix.size() == globbed_uri.key.size())
        {
            buffer.emplace_back(globbed_uri.key, std::nullopt);
            buffer_iter = buffer.begin();
            is_finished = true;
            return;
        }

        request.SetBucket(globbed_uri.bucket);
        request.SetPrefix(key_prefix);

        outcome_future = listObjectsAsync();

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_uri.key));
        recursive = globbed_uri.key == "/**" ? true : false;
        fillInternalBufferAssumeLocked();
    }

    KeyWithInfo next()
    {
        std::lock_guard lock(mutex);
        return nextAssumeLocked();
    }

    ~Impl() { list_objects_pool.wait(); }

private:
    using ListObjectsOutcome = Aws::S3::Model::ListObjectsV2Outcome;

    KeyWithInfo nextAssumeLocked()
    {
        if (buffer_iter != buffer.end())
        {
            auto answer = *buffer_iter;
            ++buffer_iter;
            return answer;
        }

        if (is_finished)
            return {};

        fillInternalBufferAssumeLocked();
        return nextAssumeLocked();
    }

    void fillInternalBufferAssumeLocked()
    {
        buffer.clear();

        assert(outcome_future.valid());
        auto outcome = outcome_future.get();

        if (!outcome.IsSuccess())
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                quoteString(request.GetBucket()),
                quoteString(request.GetPrefix()),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();

        /// It returns false when all objects were returned
        is_finished = !outcome.GetResult().GetIsTruncated();

        if (!is_finished)
        {
            /// Even if task is finished the thread may be not freed in pool.
            /// So wait until it will be freed before scheduling a new task.
            list_objects_pool.wait();
            outcome_future = listObjectsAsync();
        }

        KeysWithInfo temp_buffer;
        temp_buffer.reserve(result_batch.size());

        for (const auto & row : result_batch)
        {
            String key = row.GetKey();
            if (recursive || re2::RE2::FullMatch(key, *matcher))
            {
                S3::ObjectInfo info = {
                    .size = size_t(row.GetSize()),
                    .last_modification_time = row.GetLastModified().Millis() / 1000,
                };

                temp_buffer.emplace_back(std::move(key), std::move(info));
            }
        }

        if (temp_buffer.empty())
        {
            buffer_iter = buffer.begin();
            return;
        }

        if (!is_initialized)
        {
            createFilterAST(temp_buffer.front().key);
            is_initialized = true;
        }

        if (filter_ast)
        {
            auto block = virtual_header.cloneEmpty();
            for (size_t i = 0; i < temp_buffer.size(); ++i)
                addPathToVirtualColumns(block, fs::path(globbed_uri.bucket) / temp_buffer[i].key, i);

            VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
            const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

            buffer.reserve(block.rows());
            for (UInt64 idx : idxs.getData())
            {
                if (file_progress_callback)
                    file_progress_callback(FileProgress(0, temp_buffer[idx].info->size));
                buffer.emplace_back(std::move(temp_buffer[idx]));
            }
        }
        else
        {
            buffer = std::move(temp_buffer);
            if (file_progress_callback)
            {
                for (const auto & [_, info] : buffer)
                    file_progress_callback(FileProgress(0, info->size));
            }
        }

        /// Set iterator only after the whole batch is processed
        buffer_iter = buffer.begin();

        if (read_keys != nullptr)
            read_keys->insert(read_keys->end(), buffer.begin(), buffer.end());
    }

    void createFilterAST(const String & any_key)
    {
        if (!query || !virtual_header)
            return;

        /// Create a virtual block with one row to construct filter
        /// Append "idx" column as the filter result
        virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

        auto block = virtual_header.cloneEmpty();
        addPathToVirtualColumns(block, fs::path(globbed_uri.bucket) / any_key, 0);
        VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);
    }

    std::future<ListObjectsOutcome> listObjectsAsync()
    {
        return list_objects_scheduler(
            [this] {
                ProfileEvents::increment(ProfileEvents::S3ListObjects);
                auto outcome = client->ListObjectsV2(request);

                /// Outcome failure will be handled on the caller side.
                if (outcome.IsSuccess())
                    request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

                return outcome;
            },
            Priority{});
    }

    std::mutex mutex;

    KeysWithInfo buffer;
    KeysWithInfo::iterator buffer_iter;

    std::shared_ptr<const DB::S3::Client> client; /// proton: updated
    DB::S3::URI globbed_uri;
    ASTPtr query;
    Block virtual_header;
    bool is_initialized{false};
    ASTPtr filter_ast;
    std::unique_ptr<re2::RE2> matcher;
    bool recursive{false};
    bool is_finished{false};
    KeysWithInfo * read_keys;

    DB::S3::ListObjectsV2Request request;
    S3Settings::RequestSettings request_settings;

    ThreadPool list_objects_pool;
    ThreadPoolCallbackRunner<ListObjectsOutcome> list_objects_scheduler;
    std::future<ListObjectsOutcome> outcome_future;
    std::function<void(FileProgress)> file_progress_callback;
};

StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(
    const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
    const DB::S3::URI & globbed_uri_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    KeysWithInfo * read_keys_,
    const S3Settings::RequestSettings & request_settings_,
    std::function<void(FileProgress)> file_progress_callback_)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(
          client_, globbed_uri_, query, virtual_header, context, read_keys_, request_settings_, std::move(file_progress_callback_)))
{
}

StorageS3Source::KeyWithInfo StorageS3Source::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

class StorageS3Source::KeysIterator::Impl : WithContext
{
public:
    explicit Impl(
        const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
        const std::string & version_id_,
        const std::vector<String> & keys_,
        const String & bucket_,
        const S3Settings::RequestSettings & request_settings_,
        ASTPtr query_,
        const Block & virtual_header_,
        ContextPtr context_,
        KeysWithInfo * read_keys_,
        std::function<void(FileProgress)> file_progress_callback_)
        : WithContext(context_)
        , keys(keys_)
        , client(client_)
        , version_id(version_id_)
        , bucket(bucket_)
        , request_settings(request_settings_)
        , query(query_)
        , virtual_header(virtual_header_)
        , file_progress_callback(file_progress_callback_)
    {
        /// Create a virtual block with one row to construct filter
        if (query && virtual_header && !keys.empty())
        {
            /// Append "idx" column as the filter result
            virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

            auto block = virtual_header.cloneEmpty();
            addPathToVirtualColumns(block, fs::path(bucket) / keys.front(), 0);

            ASTPtr filter_ast;
            VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);

            if (filter_ast)
            {
                block = virtual_header.cloneEmpty();
                for (size_t i = 0; i < keys.size(); ++i)
                    addPathToVirtualColumns(block, fs::path(bucket) / keys[i], i);

                VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
                const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

                Strings filtered_keys;
                filtered_keys.reserve(block.rows());
                for (UInt64 idx : idxs.getData())
                    filtered_keys.emplace_back(std::move(keys[idx]));

                keys = std::move(filtered_keys);
            }
        }

        if (read_keys_ != nullptr)
        {
            for (const auto & key : keys)
                read_keys_->push_back({key, {}});
        }
    }

    KeyWithInfo next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};
        auto key = keys[current_index];
        std::optional<S3::ObjectInfo> info;
        if (file_progress_callback)
        {
            info = S3::getObjectInfo(*client, bucket, key, version_id, request_settings);
            file_progress_callback(FileProgress(0, info->size));
        }

        return {key, info};
    }

private:
    Strings keys;
    std::atomic_size_t index = 0;
    std::shared_ptr<const DB::S3::Client> client; /// proton: updated
    String version_id;
    String bucket;
    S3Settings::RequestSettings request_settings;
    ASTPtr query;
    Block virtual_header;
    std::function<void(FileProgress)> file_progress_callback;
};

StorageS3Source::KeysIterator::KeysIterator(
    const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
    const std::string & version_id_,
    const std::vector<String> & keys_,
    const String & bucket_,
    const S3Settings::RequestSettings & request_settings_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    KeysWithInfo * read_keys,
    std::function<void(FileProgress)> file_progress_callback_)
    : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(
          client_, version_id_, keys_, bucket_, request_settings_, query, virtual_header, context, read_keys, file_progress_callback_))
{
}

StorageS3Source::KeyWithInfo StorageS3Source::KeysIterator::next()
{
    return pimpl->next();
}

Block StorageS3Source::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

StorageS3Source::StorageS3Source(
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    const String & format_,
    String name_,
    const Block & sample_block_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const ColumnsDescription & columns_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    const String compression_hint_,
    const std::shared_ptr<const DB::S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_), true, ProcessorID::StorageS3SourceID)
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , request_settings(request_settings_)
    , compression_hint(compression_hint_)
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , requested_virtual_columns(requested_virtual_columns_)
    , file_iterator(file_iterator_)
    , download_thread_num(download_thread_num_)
    , create_reader_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "CreateS3Reader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}


void StorageS3Source::onCancel() noexcept
{
    try
    {
        client->cancel();

        std::lock_guard lock(reader_mutex);
        if (reader)
            reader->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cancel");
    }
}


StorageS3Source::ReaderHolder StorageS3Source::createReader()
{
    auto [current_key, info] = (*file_iterator)();
    if (current_key.empty())
        return {};

    size_t object_size = info ? info->size : DB::S3::getObjectSize(*client, bucket, current_key, version_id, request_settings);

    auto compression_method = chooseCompressionMethod(current_key, compression_hint);

    auto read_buf = createS3ReadBuffer(current_key, object_size);
    auto input_format = FormatFactory::instance().getInput(
        format,
        *read_buf,
        sample_block,
        getContext(),
        max_block_size,
        format_settings,
        std::nullopt,
        std::nullopt,
        /* is_remote_fs */ true,
        compression_method);

    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform([&](const Block & header) {
            return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
        });
    }

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return ReaderHolder{
        fs::path(bucket) / current_key, std::move(read_buf), std::move(input_format), std::move(pipeline), std::move(current_reader)};
}

std::future<StorageS3Source::ReaderHolder> StorageS3Source::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, Priority{});
}

std::unique_ptr<ReadBuffer> StorageS3Source::createS3ReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        LOG_TRACE(log, "Downloading object of size {} from S3 with initial prefetch", object_size);
        return createAsyncS3ReadBuffer(key, read_settings, object_size);
    }

    return std::make_unique<ReadBufferFromS3>(
        client,
        bucket,
        key,
        version_id,
        request_settings,
        read_settings,
        /*use_external_buffer*/ false,
        /*offset_*/ 0,
        /*read_until_position_*/ 0,
        /*restricted_seek_*/ false,
        object_size);
}

std::unique_ptr<ReadBuffer>
StorageS3Source::createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size)
{
    auto context = getContext();
    auto read_buffer_creator = [this, read_settings, object_size](
                                   const std::string & path, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase> {
        return std::make_unique<ReadBufferFromS3>(
            client,
            bucket,
            path,
            version_id,
            request_settings,
            read_settings,
            /* use_external_buffer */ true,
            /* offset */ 0,
            read_until_position,
            /* restricted_seek */ true,
            object_size);
    };

    auto s3_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        StoredObjects{StoredObject{key, /* local_path */ "", object_size}},
        read_settings,
        /* cache_log */ nullptr);

    auto modified_settings{read_settings};
    /// FIXME: Changing this setting to default value breaks something around parquet reading
    modified_settings.remote_read_min_bytes_for_seek = modified_settings.remote_fs_buffer_size;

    auto & pool_reader = context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    auto async_reader = std::make_unique<AsynchronousBoundedReadBuffer>(
        std::move(s3_impl), pool_reader, modified_settings, context->getAsyncReadCounters(), context->getFilesystemReadPrefetchesLog());

    async_reader->setReadUntilEnd();
    if (read_settings.remote_fs_prefetch)
        async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    return async_reader;
}

StorageS3Source::~StorageS3Source()
{
    create_reader_pool.wait();
}

String StorageS3Source::getName() const
{
    return name;
}

Chunk StorageS3Source::generate()
{
    while (true)
    {
        if (!reader || isCancelled())
            break;

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            size_t chunk_size = reader.getInputFormat()->getApproxBytesReadForChunk();
            progress(num_rows, (chunk_size != 0u) ? chunk_size : chunk.bytes());

            const auto & file_path = reader.getPath();

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    chunk.addColumn(virtual_column.type->createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = file_path.find_last_of('/');
                    auto column = virtual_column.type->createColumnConst(num_rows, file_path.substr(last_slash_pos + 1));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }
            }

            return chunk;
        }

        {
            std::lock_guard lock(reader_mutex);

            assert(reader_future.valid());
            reader = reader_future.get();

            if (!reader)
                break;

            /// Even if task is finished the thread may be not freed in pool.
            /// So wait until it will be freed before scheduling a new task.
            create_reader_pool.wait();
            reader_future = createReaderAsync();
        }
    }
    return {};
}

/// proton: starts
namespace
{
/// Adds current timestamp to the base_key (before the file extension if it has one).
String genKey(const String & base)
{
    Poco::DateTime dt;
    /// For file names have multiple extension names, like `my_file.json.gzip`, the new name will be `my_file.N.json.gzip`.
    auto pos = base.find_first_of('.');
    return fmt::format(
        "{}.{}{:02}{:02}{:02}{:02}{:02}{:03}{}",
        base.substr(0, pos),
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.millisecond(),
        (pos == std::string::npos ? "" : base.substr(pos)));
}

void updateFormatFactorySettings(FormatFactorySettings & settings, const ContextPtr & context)
{
    const auto & changes = context->getSettingsRef().changes();
    for (const auto & change : changes)
    {
        if (settings.has(change.name))
            settings.set(change.name, change.value);
    }
}

}
/// proton: ends

class StorageS3Sink final : public SinkToStorage
{
public:
    StorageS3Sink(
        const String & format_,
        const Block & sample_block_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method_,
        const StorageS3::Configuration & s3_configuration_,
        const String & bucket_,
        const String & key_,
        UInt64 min_upload_file_size_,
        UInt64 max_upload_idle_seconds)
        : SinkToStorage(sample_block_, ProcessorID::StorageS3SinkID)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , s3_configuration(s3_configuration_)
        , bucket(bucket_)
        , key(key_)
        , format_settings(format_settings_)
        , min_upload_file_size(min_upload_file_size_)
        , timer_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, max_upload_idle_seconds == 0 ? 0 : 1)
    {
        /// The timer should only start when data start flowing.
        upload_idle_timer.stop();

        next_callback = [this](size_t total_data_size) { current_total_size = total_data_size; };

        if (max_upload_idle_seconds > 0)
            timer_pool.scheduleOrThrowOnError([this, max_upload_idle_seconds]() {
                while (true)
                {
                    uint64_t sleep_time_sec = max_upload_idle_seconds;

                    {
                        std::lock_guard lock(cancel_mutex);
                        if (stopped)
                            break;

                        auto elapsed_seconds = static_cast<uint64_t>(upload_idle_timer.elapsedSeconds());
                        if (elapsed_seconds >= max_upload_idle_seconds)
                        {
                            try
                            {
                                finalize();
                            }
                            catch (...)
                            {
                                tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finalize on idle timeout");
                                release();
                            }
                        }
                        else
                        {
                            sleep_time_sec = max_upload_idle_seconds - elapsed_seconds;
                        }
                    }

                    sleepForSeconds(sleep_time_sec);
                }
            });
    }

    ~StorageS3Sink() override
    {
        try
        {
            onFinish();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finish in ~StorageS3Sink");
            release();
        }
    }

    String getName() const override { return "S3ExternalTableSink"; }

    void consume(Chunk chunk) override
    {
        /// proton: starts
        if (!chunk.hasRows())
            return;

        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;

        if (current_total_size >= min_upload_file_size && current_total_size > 0)
            /// Properly finalize the format writer and complete the current upload.
            finalize();

        if (!writer)
        {
            String current_key;

            do
            {
                current_key = genKey(key);
            } while (
                DB::S3::objectExists(*s3_configuration.client, s3_configuration.url.bucket, current_key, s3_configuration.url.version_id));

            BlobStorageLogWriterPtr blob_log = nullptr;
            if (auto blob_storage_log = context->getBlobStorageLog())
            {
                blob_log = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));
                blob_log->query_id = context->getCurrentQueryId();
            }

            write_buf = wrapWriteBufferWithCompressionMethod(
                std::make_unique<WriteBufferWithNextCallback>(
                    std::make_unique<WriteBufferFromS3>(
                        s3_configuration.client,
                        bucket,
                        current_key,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        s3_configuration.request_settings,
                        std::move(blob_log),
                        std::nullopt,
                        threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelWrite"),
                        context->getWriteSettings()),
                    next_callback),
                compression_method,
                3);
            writer
                = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, format_settings);
            writer->setAutoFlush();
        }
        /// proton: ends

        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
        /// proton: starts
        /// Restart the timer when there are new data, because we are calculating the idle time.
        upload_idle_timer.start();
        /// proton: ends
    }

    void onCancel() noexcept override
    {
        /// Avoid endless retry that blocks the cancellation
        try
        {
            s3_configuration.client->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to cancel S3 client");
        }

        std::lock_guard lock(cancel_mutex);
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finalize on cancel");
            release();
        }
        cancelled = true;
        stopped = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization
            release();
        }
    }

    void onFinish() override
    {
        /// Avoid endless retry that blocks finishing the job.
        s3_configuration.client->cancel();

        std::lock_guard lock(cancel_mutex);
        finalize();
        stopped = true;
    }

private:
    void finalize()
    {
        if (!writer)
            return;

        /// Stop ParallelFormattingOutputFormat correctly.
        SCOPE_EXIT(writer.reset());

        writer->finalize();
        writer->flush();
        write_buf->finalize();

        /// proton: starts
        current_total_size = 0;
        upload_idle_timer.reset();
        /// proton: ends
    }

    void release()
    {
        writer.reset();
        write_buf.reset();
    }

    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;
    const StorageS3::Configuration s3_configuration;
    String bucket;
    const String key;
    std::optional<FormatSettings> format_settings;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;
    /// proton: starts
    size_t current_total_size = 0;
    UInt64 min_upload_file_size = 0;

    ASTPtr file_exprssion_ast;

    bool stopped = false;
    ThreadPool timer_pool;
    Stopwatch upload_idle_timer;

    std::function<void(size_t)> next_callback;
    /// proton: ends
};


class PartitionedStorageS3Sink : public PartitionedSink
{
public:
    PartitionedStorageS3Sink(
        const ASTPtr & partition_by,
        const String & format_,
        const Block & sample_block_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method_,
        const StorageS3::Configuration s3_configuration_,
        const String & bucket_,
        const String & key_,
        /// proton: starts
        UInt64 min_upload_file_size_,
        UInt64 max_upload_idle_seconds_)
        /// proton: ends
        : PartitionedSink(partition_by, context_, sample_block_, ProcessorID::StorageS3SinkID)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , s3_configuration(s3_configuration_)
        , bucket(bucket_)
        , key(key_)
        , format_settings(format_settings_)
        /// proton: starts
        , min_upload_file_size(min_upload_file_size_)
        , max_upload_idle_seconds(max_upload_idle_seconds_)
        /// There should not be too many on-going sinks at the same time.
        /// We can make this configurable when it needs to be.
        , sinks_cache(/*max_size_=*/100)
    /// proton: ends
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_bucket = replaceWildcards(bucket, partition_id);
        validateBucket(partition_bucket);

        auto partition_key = replaceWildcards(key, partition_id);
        validateKey(partition_key);

        return std::make_shared<StorageS3Sink>(
            format,
            sample_block,
            context,
            format_settings,
            compression_method,
            s3_configuration,
            partition_bucket,
            partition_key,
            /// proton: starts
            min_upload_file_size,
            max_upload_idle_seconds);
        /// proton: ends
    }

private:
    const String format;
    const Block sample_block;
    const ContextPtr context;
    const CompressionMethod compression_method;
    const StorageS3::Configuration s3_configuration;
    const String bucket;
    const String key;
    const std::optional<FormatSettings> format_settings;
    UInt64 min_upload_file_size = 0; /// proton: updated
    UInt64 max_upload_idle_seconds = 0; /// proton: updated

    ExpressionActionsPtr partition_by_expr;

    LRUCache<String, SinkToStorage> sinks_cache;

    static void validateBucket(const String & str)
    {
        DB::S3::URI::validateBucket(str, {});

        if (DB::UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()) == 0u)
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

        validatePartitionKey(str, false);
    }

    static void validateKey(const String & str)
    {
        /// See:
        /// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        /// - https://cloud.ibm.com/apidocs/cos/cos-compatibility#putobject

        if (str.empty() || str.size() > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect key length (not empty, max 1023 characters), got: {}", str.size());

        if (DB::UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()) == 0u)
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in key");

        validatePartitionKey(str, true);
    }
};


StorageS3::StorageS3(
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatFactorySettings> format_factory_settings_,
    const String & select_key_,
    const String & insert_key_,
    bool attach,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , s3_configuration{configuration_}
    , select_key(select_key_)
    , insert_key(insert_key_)
    , min_upload_file_size(configuration_.min_upload_file_size)
    , max_upload_idle_seconds(configuration_.max_upload_idle_seconds)
    , format_name(configuration_.format)
    , compression_method(configuration_.compression_method)
    , name(s3_configuration.url.storage_name)
    , distributed_processing(distributed_processing_)
    , format_factory_settings(format_factory_settings_)
    , partition_by(partition_by_)
    , is_key_with_globs(s3_configuration.url.key.find_first_of("*?{") != std::string::npos)
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(s3_configuration.url.uri);
    StorageInMemoryMetadata storage_metadata;

    if (!attach)
    {
        updateConfiguration(context_, s3_configuration);

        auto bucket_exists = false;

        try
        {
            bucket_exists = s3_configuration.client->bucketExists(s3_configuration.url.bucket);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("Could not validate bucket {}", s3_configuration.url.bucket));
            e.rethrow();
        }

        if (!bucket_exists)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Bucket {} does not exist", s3_configuration.url.bucket);
    }

    if (columns_.empty())
    {
        auto columns = getTableStructureFromDataImpl(
            format_name,
            s3_configuration,
            compression_method,
            distributed_processing_,
            is_key_with_globs,
            getFormatSettings(context_),
            context_);

        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

std::shared_ptr<StorageS3Source::IIterator> StorageS3::createFileIterator(
    Configuration s3_configuration,
    bool is_key_with_globs,
    bool distributed_processing,
    ContextPtr local_context,
    ASTPtr query,
    const Block & virtual_block,
    KeysWithInfo * read_keys,
    std::function<void(FileProgress)> file_progress_callback)
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::ReadTaskIterator>(local_context->getReadTaskCallback());
    }
    else if (is_key_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<StorageS3Source::DisclosedGlobIterator>(
            s3_configuration.client,
            s3_configuration.url,
            query,
            virtual_block,
            local_context,
            read_keys,
            s3_configuration.request_settings,
            file_progress_callback);
    }
    else
    {
        std::vector<String> keys{s3_configuration.url.key};

        return std::make_shared<StorageS3Source::KeysIterator>(
            s3_configuration.client,
            s3_configuration.url.version_id,
            keys,
            s3_configuration.url.bucket,
            s3_configuration.request_settings,
            query,
            virtual_block,
            local_context,
            read_keys,
            file_progress_callback);
    }
}

bool StorageS3::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context_);
}

Pipe StorageS3::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    /// Replace the `{partition_id}` wildcard with `**`, so it can read files written with partitions.
    /// It's possible that this includes more files than expected, but this is the best guess, and users always can overwrite
    /// it with the `s3_file` setting.
    if (select_key.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "S3 external stream without the select_key setting is write-only.");

    auto query_s3_configuration = copyAndUpdateConfiguration(local_context, s3_configuration);
    query_s3_configuration.url.key = select_key; /// proton: added

    Pipes pipes;
    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = createFileIterator( /// proton: updated
        query_s3_configuration,
        /*is_key_with_globs=*/query_s3_configuration.url.key.find_first_of("*?{") != std::string::npos,
        distributed_processing,
        local_context,
        query_info.query,
        virtual_block);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns(local_context))
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(fetch_columns, [&](const String & col) {
            return std::any_of(
                virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col) { return col == virtual_col.name; });
        });

        if (fetch_columns.empty())
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()));

        columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    const size_t max_download_threads = local_context->getSettingsRef().max_download_threads;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>( /// proton: updated
            requested_virtual_columns,
            format_name,
            getName(),
            block_for_format,
            local_context,
            getFormatSettings(local_context),
            columns_description,
            max_block_size,
            query_s3_configuration.request_settings,
            compression_method,
            query_s3_configuration.client,
            query_s3_configuration.url.bucket,
            query_s3_configuration.url.version_id,
            iterator_wrapper,
            max_download_threads));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    narrowPipe(pipe, num_streams);
    return pipe;
}

/// proton: starts
FormatSettings StorageS3::getFormatSettings(const ContextPtr & local_context) const
{
    FormatFactorySettings settings = format_factory_settings.has_value() ? *format_factory_settings : FormatFactorySettings();
    updateFormatFactorySettings(settings, local_context);
    auto format_settings = DB::getFormatSettings(local_context, settings);

    /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
    /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;
    return format_settings;
}
/// proton: ends

SinkToStoragePtr StorageS3::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    /// proton: starts
    if (insert_key.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "S3 external stream without the write_to setting is read-only.");

    auto query_s3_configuration = copyAndUpdateConfiguration(local_context, s3_configuration);

    auto min_upload_file_size_ = local_context->getSettingsRef().s3_min_upload_file_size.changed
        ? local_context->getSettingsRef().s3_min_upload_file_size.value
        : min_upload_file_size;
    auto max_upload_idle_seconds_ = local_context->getSettingsRef().s3_max_upload_idle_seconds.changed
        ? local_context->getSettingsRef().s3_max_upload_idle_seconds.value
        : max_upload_idle_seconds;
    /// proton: ends

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(insert_key, compression_method); /// proton: updated
    bool has_wildcards = query_s3_configuration.url.bucket.find(PARTITION_ID_WILDCARD) != String::npos
        || insert_key.find(PARTITION_ID_WILDCARD) != String::npos; /// proton: updated
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by)
                                         : (local_context->getCreator().empty() ? nullptr : partition_by);
    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    /// proton: starts
    auto format_settings = getFormatSettings(local_context);

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageS3Sink>(
            partition_by_ast,
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            query_s3_configuration,
            query_s3_configuration.url.bucket,
            insert_key,
            min_upload_file_size_,
            max_upload_idle_seconds_);
    }
    else
    {
        return std::make_shared<StorageS3Sink>(
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            query_s3_configuration,
            query_s3_configuration.url.bucket,
            insert_key,
            min_upload_file_size_,
            max_upload_idle_seconds_);
    }
    /// proton: ends
}

void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    auto query_s3_configuration = copyAndUpdateConfiguration(local_context, s3_configuration);

    /// proton: starts

    if (select_key.find_first_of("*?{") != std::string::npos)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "S3 key '{}' contains globs, cannot truncate", select_key);

    Aws::S3::Model::Delete delkeys;

    /*for (const auto & key : keys)*/
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(select_key);
        delkeys.AddObjects(std::move(obj));
    }

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    DB::S3::DeleteObjectsRequest request;
    request.SetBucket(query_s3_configuration.url.bucket);
    request.SetDelete(delkeys);

    auto response = query_s3_configuration.client->DeleteObjects(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(ErrorCodes::S3_ERROR, "{}:{}", static_cast<int>(err.GetErrorType()), err.GetMessage());
    }

    for (const auto & error : response.GetResult().GetErrors())
        LOG_WARNING(getLogger("StorageS3"), " Failed to delete {}, error: {}", error.GetKey(), error.GetMessage());
}

StorageS3::Configuration StorageS3::copyAndUpdateConfiguration(ContextPtr local_context, const StorageS3::Configuration & configuration)
{
    StorageS3::Configuration new_configuration(configuration);
    updateConfiguration(local_context, new_configuration);
    return new_configuration;
}

void StorageS3::updateConfiguration(ContextPtr ctx, StorageS3::Configuration & upd)
{
    if (upd.static_configuration && upd.client)
        return;

    upd.request_settings.updateFromSettings(ctx->getSettings());

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        upd.auth_settings.region,
        ctx->getRemoteHostFilter(),
        static_cast<unsigned>(ctx->getGlobalContext()->getSettingsRef().s3_max_redirects),
        ctx->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        upd.request_settings.get_request_throttler,
        upd.request_settings.put_request_throttler);

    /// Using "https://s3.amazonaws.com" will cause some issues on some API, like HeadBucket.
    /// No need to override the endpoint in this case.
    if (upd.url.endpoint != "https://s3.amazonaws.com")
        client_configuration.endpointOverride = upd.url.endpoint;
    client_configuration.maxConnections = static_cast<unsigned>(upd.request_settings.max_connections);

    auto credentials = Aws::Auth::AWSCredentials(upd.auth_settings.access_key_id, upd.auth_settings.secret_access_key);
    auto headers = upd.auth_settings.headers;
    if (!upd.headers_from_ast.empty())
        headers.insert(headers.end(), upd.headers_from_ast.begin(), upd.headers_from_ast.end());

    client_configuration.retryStrategy
        = std::make_unique<Aws::Client::StandardRetryStrategy>(upd.request_settings.max_aws_request_failure_retries);

    upd.client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        upd.url.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        upd.auth_settings.server_side_encryption_customer_key_base64,
        upd.auth_settings.server_side_encryption_kms_config,
        std::move(headers),
        S3::CredentialsConfiguration{
            upd.auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
            upd.auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            upd.auth_settings.expiration_window_seconds.value_or(
                ctx->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            upd.auth_settings.no_sign_request.value_or(ctx->getConfigRef().getBool("s3.no_sign_request", false)),
        });
}


void StorageS3::processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        configuration.url = DB::S3::URI(std::filesystem::path(collection.get<String>("url")) / filename);
    else
        configuration.url = DB::S3::URI(collection.get<String>("url"));

    configuration.auth_settings.access_key_id = collection.getOrDefault<String>("access_key_id", "");
    configuration.auth_settings.secret_access_key = collection.getOrDefault<String>("secret_access_key", "");
    configuration.auth_settings.use_environment_credentials = collection.getOrDefault<UInt64>("use_environment_credentials", 0);

    configuration.format = collection.getOrDefault<String>("format", "auto");
    configuration.compression_method
        = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    configuration.structure = collection.getOrDefault<String>("structure", "auto");

    configuration.request_settings = S3Settings::RequestSettings(collection);
}


StorageS3::Configuration StorageS3::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageS3::Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        processNamedCollectionResult(configuration, *named_collection);
    }
    else
    {
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key')
        /// with optional headers() function
        if (engine_args.empty() || engine_args.size() > 5)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage S3 requires 1 to 5 arguments: url, [access_key_id, secret_access_key], name of used format and "
                "[compression_method]");

        auto header_it = StorageURL::collectHeaders(engine_args, configuration.headers_from_ast, local_context);
        if (header_it != engine_args.end())
            engine_args.erase(header_it);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        /// Size -> argument indexes
        static std::unordered_map<size_t, std::unordered_map<std::string_view, size_t>> size_to_engine_args{
            {1, {{}}},
            {2, {{"format", 1}}},
            {4, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}}},
            {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression_method", 4}}}};

        std::unordered_map<std::string_view, size_t> engine_args_to_idx;
        /// For 3 arguments we support 2 possible variants:
        /// s3(source, format, compression_method) and s3(source, access_key_id, access_key_id)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        if (engine_args.size() == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};

            else
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        else
        {
            engine_args_to_idx = size_to_engine_args[engine_args.size()];
        }

        /// This argument is always the first
        configuration.url = DB::S3::URI(checkAndGetLiteralArgument<String>(engine_args[0], "url"));

        if (engine_args_to_idx.contains("format"))
            configuration.format = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["format"]], "format");

        if (engine_args_to_idx.contains("compression_method"))
            configuration.compression_method
                = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["compression_method"]], "compression_method");

        if (engine_args_to_idx.contains("access_key_id"))
            configuration.auth_settings.access_key_id
                = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["access_key_id"]], "access_key_id");

        if (engine_args_to_idx.contains("secret_access_key"))
            configuration.auth_settings.secret_access_key
                = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["secret_access_key"]], "secret_access_key");
    }

    configuration.static_configuration = !configuration.auth_settings.access_key_id.empty();

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url.key, true);

    return configuration;
}

ColumnsDescription StorageS3::getTableStructureFromData(
    const StorageS3::Configuration & configuration,
    bool distributed_processing,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    auto query_s3_configuration = copyAndUpdateConfiguration(ctx, configuration);
    return getTableStructureFromDataImpl(
        query_s3_configuration.format,
        query_s3_configuration,
        query_s3_configuration.compression_method,
        distributed_processing,
        query_s3_configuration.url.key.find_first_of("*?{") != std::string::npos,
        format_settings,
        ctx);
}

ColumnsDescription StorageS3::getTableStructureFromDataImpl(
    const String & format,
    const Configuration & s3_configuration,
    const String & compression_method,
    bool distributed_processing,
    bool is_key_with_globs,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    KeysWithInfo read_keys;

    auto file_iterator = createFileIterator(s3_configuration, is_key_with_globs, distributed_processing, ctx, nullptr, {}, &read_keys);

    std::optional<ColumnsDescription> columns_from_cache;
    size_t prev_read_keys_size = read_keys.size();
    if (ctx->getSettingsRef().schema_inference_use_cache_for_s3)
        columns_from_cache = tryGetColumnsFromCache(read_keys.begin(), read_keys.end(), s3_configuration, format, format_settings, ctx);

    ReadBufferIterator read_buffer_iterator
        = [&, first = true](ColumnsDescription & cached_columns) mutable -> std::unique_ptr<ReadBuffer> {
        auto [key, _] = (*file_iterator)();

        if (key.empty())
        {
            if (first)
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_STREAM_STRUCTURE,
                    "Cannot extract stream structure from {} format file, because there are no files with provided path in S3. You must "
                    "specify " /// proton: updated
                    "stream structure manually", /// proton: updated
                    format);

            return nullptr;
        }

        /// S3 file iterator could get new keys after new iteration, check them in schema cache.
        if (ctx->getSettingsRef().schema_inference_use_cache_for_s3 && read_keys.size() > prev_read_keys_size)
        {
            columns_from_cache = tryGetColumnsFromCache(
                read_keys.begin() + prev_read_keys_size, read_keys.end(), s3_configuration, format, format_settings, ctx);
            prev_read_keys_size = read_keys.size();
            if (columns_from_cache)
            {
                cached_columns = *columns_from_cache;
                return nullptr;
            }
        }

        first = false;
        int zstd_window_log_max = static_cast<int>(ctx->getSettingsRef().zstd_window_log_max);
        return wrapReadBufferWithCompressionMethod(
            std::make_unique<ReadBufferFromS3>(
                s3_configuration.client,
                s3_configuration.url.bucket,
                key,
                s3_configuration.url.version_id,
                s3_configuration.request_settings,
                ctx->getReadSettings()),
            chooseCompressionMethod(key, compression_method),
            zstd_window_log_max);
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(format, format_settings, read_buffer_iterator, is_key_with_globs, ctx);

    if (ctx->getSettingsRef().schema_inference_use_cache_for_s3)
        addColumnsToCache(read_keys, s3_configuration, columns, format, format_settings, ctx);

    return columns;
}

NamesAndTypesList StorageS3::getVirtuals() const
{
    return virtual_columns;
}

bool StorageS3::supportsPartitionBy() const
{
    return true;
}

SchemaCache & StorageS3::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(
        ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_s3", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> StorageS3::tryGetColumnsFromCache(
    const KeysWithInfo::const_iterator & begin,
    const KeysWithInfo::const_iterator & end,
    const Configuration & s3_configuration,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    for (auto it = begin; it < end; ++it)
    {
        auto get_last_mod_time = [&] {
            time_t last_modification_time = 0;
            if (it->info)
            {
                last_modification_time = it->info->last_modification_time;
            }
            else
            {
                /// Note that in case of exception in getObjectInfo returned info will be empty,
                /// but schema cache will handle this case and won't return columns from cache
                /// because we can't say that it's valid without last modification time.
                last_modification_time = S3::getObjectInfo(
                                             *s3_configuration.client,
                                             s3_configuration.url.bucket,
                                             it->key,
                                             s3_configuration.url.version_id,
                                             s3_configuration.request_settings,
                                             /*with_metadata=*/false,
                                             /*for_disk_s3=*/false,
                                             /*throw_on_error= */ false)
                                             .last_modification_time;
            }

            return (last_modification_time != 0) ? std::make_optional(last_modification_time) : std::nullopt;
        };

        String path = fs::path(s3_configuration.url.bucket) / it->key;
        String source = fs::path(s3_configuration.url.uri.getHost() + std::to_string(s3_configuration.url.uri.getPort())) / path;
        auto cache_key = getKeyForSchemaCache(source, format_name, format_settings, ctx);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void StorageS3::addColumnsToCache(
    const KeysWithInfo & keys,
    const Configuration & s3_configuration,
    const ColumnsDescription & columns,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    auto host_and_bucket
        = fs::path(s3_configuration.url.uri.getHost() + std::to_string(s3_configuration.url.uri.getPort())) / s3_configuration.url.bucket;
    Strings sources;
    sources.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(sources), [&](const auto & elem) { return host_and_bucket / elem.key; });
    auto cache_keys = getKeysForSchemaCache(sources, format_name, format_settings, ctx);
    auto & schema_cache = getSchemaCache(ctx);
    schema_cache.addMany(cache_keys, columns);
}

}

}

#endif
