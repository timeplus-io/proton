#include <Storages/StorageURL.h>
#include <Storages/PartitionedSink.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/IOThreadPool.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/WriteBufferFromHTTP.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>

#include <Common/ThreadStatus.h>
#include <Common/parseRemoteDescription.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/re2.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <IO/ReadWriteBufferFromHTTP.h>

#include <algorithm>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Net/HTTPRequest.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <regex>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_EXTRACT_STREAM_STRUCTURE;
}

static constexpr auto bad_arguments_error_message = "Storage URL requires 1-4 arguments: "
    "url, name of used format (taken from file extension by default), "
    "optional compression method, optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "filename",
    "method",
    "http_method",
    "description",
    "headers.header.name",
    "headers.header.value",
};


static bool urlWithGlobs(const String & uri)
{
    return (uri.find('{') != std::string::npos && uri.find('}') != std::string::npos) || uri.find('|') != std::string::npos;
}

static ConnectionTimeouts getHTTPTimeouts(ContextPtr context)
{
    return ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), {context->getConfigRef().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT), 0});
}

IStorageURLBase::IStorageURLBase(
    const String & uri_,
    ContextPtr context_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const String & compression_method_,
    const HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , uri(uri_)
    , compression_method(chooseCompressionMethod(Poco::URI(uri_).getPath(), compression_method_))
    , format_name(format_name_)
    , format_settings(format_settings_)
    , headers(headers_)
    , http_method(http_method_)
    , partition_by(partition_by_)
{
    FormatFactory::instance().checkFormatName(format_name);
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(format_name, uri, compression_method, headers, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
    {
        /// We don't allow special columns in URL storage.
        if (!columns_.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine URL doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


namespace
{
    HTTPHeaderEntries getHeaders(const HTTPHeaderEntries & headers_)
    {
        HTTPHeaderEntries headers(headers_.begin(), headers_.end());

        // Propagate OpenTelemetry trace context, if any, downstream.
        const auto &current_trace_context = OpenTelemetry::CurrentContext();
        if (current_trace_context.isTraceEnabled())
        {
            headers.emplace_back("traceparent", current_trace_context.composeTraceparentHeader());

            if (!current_trace_context.tracestate.empty())
            {
                headers.emplace_back("tracestate", current_trace_context.tracestate);
            }
        }

        return headers;
    }
}

void StorageURLSource::onCancel() noexcept
{
    std::lock_guard lock(reader_mutex);
    if (reader)
    {
        try
        {
            reader->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("URLSource"), "Failed to cancel reader");
        }
    }
}

void StorageURLSource::setCredentials(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & request_uri)
{
    const auto & user_info = request_uri.getUserInfo();
    if (!user_info.empty())
    {
        std::size_t n = user_info.find(':');
        if (n != std::string::npos)
        {
            credentials.setUsername(user_info.substr(0, n));
            credentials.setPassword(user_info.substr(n + 1));
        }
    }
}

StorageURLSource::StorageURLSource(
    const ReadFromFormatInfo & info,
    URIInfoPtr uri_info_,
    const std::string & http_method,
    std::function<void(std::ostream &)> callback,
    const String & format,
    const std::optional<FormatSettings> & format_settings,
    String name_,
    ContextPtr context,
    UInt64 max_block_size,
    const ConnectionTimeouts & timeouts,
    CompressionMethod compression_method,
    size_t max_parsing_threads,
    const HTTPHeaderEntries & headers_,
    const URIParams & params,
    bool glob_url,
    bool need_only_count_)
    : ISource(info.source_header, false, ProcessorID::StorageURLSourceID)
    , name(std::move(name_))
    , columns_description(info.columns_description)
    , requested_columns(info.requested_columns)
    , block_for_format(info.format_header)
    , uri_info(uri_info_)
    , need_only_count(need_only_count_)
{
    auto headers = getHeaders(headers_);

    /// Lazy initialization. We should not perform requests in constructor, because we need to do it in query pipeline.
    initialize = [=, this](const URIInfo::FailoverOptions & uri_options) {
        if (uri_options.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty url list");

        auto first_option = uri_options.begin();
        auto [actual_uri, buf] = getFirstAvailableURIAndReadBuffer(
            first_option,
            uri_options.end(),
            context,
            params,
            http_method,
            callback,
            timeouts,
            credentials,
            headers,
            glob_url,
            uri_options.size() == 1);

        read_buf = std::move(buf);

        if (auto file_progress_callback = context->getFileProgressCallback())
        {
            size_t file_size = tryGetFileSizeFromReadBuffer(*read_buf).value_or(0);
            LOG_DEBUG(getLogger("URL"), "Send file size {}", file_size);
            file_progress_callback(FileProgress(0, file_size));
        }

        // TODO: Pass max_parsing_threads and max_download_threads adjusted for num_streams.
        input_format = FormatFactory::instance().getInput(
            format,
            *read_buf,
            block_for_format,
            context,
            max_block_size,
            format_settings,
            need_only_count ? 1 : max_parsing_threads,
            /*max_download_threads*/ std::nullopt,
            /* is_remote_fs */ true,
            compression_method);

        if (need_only_count)
            input_format->needOnlyCount();

        QueryPipelineBuilder builder;
        builder.init(Pipe(input_format));

        if (columns_description.hasDefaults())
        {
            builder.addSimpleTransform([&](const Block & cur_header) {
                return std::make_shared<AddingDefaultsTransform>(cur_header, columns_description, *input_format, context);
            });
        }

        /// Add ExtractColumnsTransform to extract requested columns/subcolumns
        /// from chunk read by IInputFormat.
        builder.addSimpleTransform(
            [&](const Block & header) { return std::make_shared<ExtractColumnsTransform>(header, requested_columns); });

        pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
    };
}

StorageURLSource::~StorageURLSource() = default;

Chunk StorageURLSource::generate()
{
    while (true)
    {
        if (!reader)
        {
            auto current_uri_pos = uri_info->next_uri_to_read.fetch_add(1);
            if (current_uri_pos >= uri_info->uri_list_to_read.size())
                return {};

            auto current_uri = uri_info->uri_list_to_read[current_uri_pos];

            std::lock_guard lock(reader_mutex);
            initialize(current_uri);
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            size_t chunk_size = input_format->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());

            return chunk;
        }

        {
            std::lock_guard lock(reader_mutex);
            pipeline->reset();
            reader.reset();
        }
    }
}

std::tuple<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> StorageURLSource::getFirstAvailableURIAndReadBuffer(
    std::vector<String>::const_iterator & option,
    const std::vector<String>::const_iterator & end,
    ContextPtr context_,
    const URIParams & params,
    const String & http_method,
    std::function<void(std::ostream &)> callback,
    const ConnectionTimeouts & timeouts,
    Poco::Net::HTTPBasicCredentials & credentials,
    const HTTPHeaderEntries & headers,
    bool glob_url,
    bool delay_initialization)
{
    String first_exception_message;
    ReadSettings read_settings = context_->getReadSettings();

    size_t options = std::distance(option, end);
    for (; option != end; ++option)
    {
        bool skip_url_not_found_error = glob_url && read_settings.http_skip_not_found_url_for_globs && option == std::prev(end);
        auto request_uri = Poco::URI(*option);

        for (const auto & [param, value] : params)
            request_uri.addQueryParameter(param, value);

        setCredentials(credentials, request_uri);

        const auto & settings = context_->getSettingsRef();

        try
        {
            auto res = BuilderRWBufferFromHTTP(request_uri)
                           .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                           .withMethod(http_method)
                           .withSettings(read_settings)
                           .withTimeouts(timeouts)
                           .withHostFilter(&context_->getRemoteHostFilter())
                           .withBufSize(settings.max_read_buffer_size)
                           .withRedirects(settings.max_http_get_redirects)
                           .withOutCallback(callback)
                           .withSkipNotFound(skip_url_not_found_error)
                           .withHeaders(headers)
                           .withDelayInit(delay_initialization)
                           .create(credentials);

            return std::make_tuple(request_uri, std::move(res));
        }
        catch (...)
        {
            if (options == 1)
                throw;

            if (first_exception_message.empty())
                first_exception_message = getCurrentExceptionMessage(false);

            tryLogCurrentException(__PRETTY_FUNCTION__);

            continue;
        }
    }

    throw Exception(ErrorCodes::NETWORK_ERROR, "All uri ({}) options are unreachable: {}", options, first_exception_message);
}

StorageURLSink::StorageURLSink(
    const String & uri,
    const String & format,
    const std::optional<FormatSettings> & format_settings,
    const Block & sample_block,
    ContextPtr context,
    const ConnectionTimeouts & timeouts,
    const CompressionMethod compression_method,
    const String & http_method)
    : SinkToStorage(sample_block, ProcessorID::StorageURLSinkID)
{
    std::string content_type = FormatFactory::instance().getContentType(format, context, format_settings);
    std::string content_encoding = toContentEncodingName(compression_method);

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::make_unique<WriteBufferFromHTTP>(
            HTTPConnectionGroupType::STORAGE,
            Poco::URI(uri),
            http_method,
            content_type,
            content_encoding,
            HTTPHeaderEntries{},
            timeouts),
        compression_method,
        3);
    writer = FormatFactory::instance().getOutputFormat(format, *write_buf, sample_block, context, format_settings);
}


void StorageURLSink::consume(Chunk chunk)
{
    if (isCancelled())
        return;
    writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void StorageURLSink::onFinish()
{
    finalizeBuffers();
    releaseBuffers();
}

void StorageURLSink::finalizeBuffers()
{
    if (!writer)
        return;

    try
    {
        writer->finalize();
        writer->flush();
        write_buf->finalize();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        releaseBuffers();
        throw;
    }
}

void StorageURLSink::releaseBuffers()
{
    writer.reset();
    write_buf->finalize();
}

void StorageURLSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

class PartitionedStorageURLSink final : public PartitionedSink
{
public:
    PartitionedStorageURLSink(
        const ASTPtr & partition_by,
        const String & uri_,
        const String & format_,
        const std::optional<FormatSettings> & format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ConnectionTimeouts & timeouts_,
        const CompressionMethod compression_method_,
        const String & http_method_,
        ProcessorID pid_)
        : PartitionedSink(partition_by, context_, sample_block_, pid_)
        , uri(uri_)
        , format(format_)
        , format_settings(format_settings_)
        , sample_block(sample_block_)
        , context(context_)
        , timeouts(timeouts_)
        , compression_method(compression_method_)
        , http_method(http_method_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_path = PartitionedSink::replaceWildcards(uri, partition_id);
        context->getRemoteHostFilter().checkURL(Poco::URI(partition_path));
        return std::make_shared<StorageURLSink>(
            partition_path, format, format_settings, sample_block, context, timeouts, compression_method, http_method);
    }

private:
    const String uri;
    const String format;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    ContextPtr context;
    const ConnectionTimeouts timeouts;

    const CompressionMethod compression_method;
    const String http_method;
};

std::string IStorageURLBase::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_GET;
}

std::vector<std::pair<std::string, std::string>> IStorageURLBase::getReadURIParams(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return {};
}

std::function<void(std::ostream &)> IStorageURLBase::getReadPOSTDataCallback(
    const Names & /*column_names*/,
    const ColumnsDescription & /* columns_description */,
    const SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return nullptr;
}

ColumnsDescription IStorageURLBase::getTableStructureFromData(
    const String & format,
    const String & uri,
    CompressionMethod compression_method,
    const HTTPHeaderEntries & headers,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    Poco::Net::HTTPBasicCredentials credentials;

    std::vector<String> urls_to_check;
    if (urlWithGlobs(uri))
    {
        size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
        auto uri_descriptions = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses);
        for (const auto & description : uri_descriptions)
        {
                auto options = parseRemoteDescription(description, 0, description.size(), '|', max_addresses);
                urls_to_check.insert(urls_to_check.end(), options.begin(), options.end());
        }
    }
    else
    {
        urls_to_check = {uri};
    }

    std::optional<ColumnsDescription> columns_from_cache;
    if (context->getSettingsRef().schema_inference_use_cache_for_url)
        columns_from_cache = tryGetColumnsFromCache(urls_to_check, headers, credentials, format, format_settings, context);

    ReadBufferIterator read_buffer_iterator = [&, it = urls_to_check.cbegin(), first = true](ColumnsDescription &) mutable -> std::unique_ptr<ReadBuffer>
    {
        std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> uri_and_buf;
        do
        {
            if (it == urls_to_check.cend())
            {
                if (first)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_STREAM_STRUCTURE,
                        "Cannot extract stream structure from {} format file, because all files are empty. "
                        "You must specify stream structure manually",
                        format);
                return nullptr;
            }

            uri_and_buf = StorageURLSource::getFirstAvailableURIAndReadBuffer(
                it,
                urls_to_check.cend(),
                context,
                {},
                Poco::Net::HTTPRequest::HTTP_GET,
                {},
                getHTTPTimeouts(context),
                credentials,
                headers,
                false,
                false);

            ++it;
        } while (context->getSettingsRef().engine_url_skip_empty_files && uri_and_buf.second->eof());

        first = false;
        return wrapReadBufferWithCompressionMethod(
            std::move(uri_and_buf.second),
            compression_method,
            static_cast<int>(context->getSettingsRef().zstd_window_log_max));
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(format, format_settings, read_buffer_iterator, urls_to_check.size() > 1, context);

    if (context->getSettingsRef().schema_inference_use_cache_for_url)
        addColumnsToCache(urls_to_check, columns, format, format_settings, context);

    return columns;
}

bool IStorageURLBase::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context, format_settings);
}

bool IStorageURLBase::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(format_name);
}

bool IStorageURLBase::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(format_name, context);
}

Pipe IStorageURLBase::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns(local_context))
    {
        columns_description = storage_snapshot->getDescriptionForColumns(column_names);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    auto uri_info = std::make_shared<StorageURLSource::URIInfo>();
    bool is_url_with_globs = urlWithGlobs(uri);
    size_t max_addresses = local_context->getSettingsRef().glob_expansion_max_elements;
    if (is_url_with_globs)
    {
        auto uri_descriptions = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses);
        /// For each uri (which acts like shard) check if it has failover options
        for (const auto & description : uri_descriptions)
            uri_info->uri_list_to_read.emplace_back(parseRemoteDescription(description, 0, description.size(), '|', max_addresses));

        if (num_streams > uri_descriptions.size())
            num_streams = uri_descriptions.size();
    }
    else
    {
        uri_info->uri_list_to_read.emplace_back(std::vector<String>{uri});
        num_streams = 1;
    }

    auto read_from_format_info
        = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    Pipes pipes;
    pipes.reserve(num_streams);

    const size_t max_threads = local_context->getSettingsRef().max_threads;
    size_t max_parsing_threads = num_streams >= max_threads ? 1 : (max_threads / num_streams);

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageURLSource>(
            read_from_format_info,
            uri_info,
            getReadMethod(),
            getReadPOSTDataCallback(
                read_from_format_info.columns_description.getNamesOfPhysical(),
                read_from_format_info.columns_description,
                query_info,
                local_context,
                processed_stage,
                max_block_size),
            format_name,
            format_settings,
            getName(),
            local_context,
            max_block_size,
            getHTTPTimeouts(local_context),
            compression_method,
            max_parsing_threads,
            headers,
            params,
            is_url_with_globs,
            need_only_count));
    }

    return Pipe::unitePipes(std::move(pipes));
}


Pipe StorageURLWithFailover::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns(local_context))
    {
        columns_description = storage_snapshot->getDescriptionForColumns(column_names);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

    auto uri_info = std::make_shared<StorageURLSource::URIInfo>();
    uri_info->uri_list_to_read.emplace_back(uri_options);

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());

    const size_t max_threads = local_context->getSettingsRef().max_threads;
    const size_t max_parsing_threads = num_streams >= max_threads ? 1 : (max_threads / num_streams);

    auto pipe = Pipe(std::make_shared<StorageURLSource>(
        read_from_format_info,
        uri_info,
        getReadMethod(),
        getReadPOSTDataCallback(read_from_format_info.columns_description.getNamesOfPhysical(), read_from_format_info.columns_description, query_info, local_context, processed_stage, max_block_size),
        format_name,
        format_settings,
        getName(),
        local_context,
        max_block_size,
        getHTTPTimeouts(local_context),
        compression_method,
        max_parsing_threads,
        headers,
        params));
    std::shuffle(uri_options.begin(), uri_options.end(), thread_local_rng);
    return pipe;
}


SinkToStoragePtr IStorageURLBase::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    if (http_method.empty())
        http_method = Poco::Net::HTTPRequest::HTTP_POST;

    bool has_wildcards = uri.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageURLSink>(
            partition_by_ast,
            uri,
            format_name,
            format_settings,
            metadata_snapshot->getSampleBlock(),
            context,
            getHTTPTimeouts(context),
            compression_method,
            http_method,
            ProcessorID::PartitionedStorageURLSinkID);
    }
    else
    {
        return std::make_shared<StorageURLSink>(
            uri,
            format_name,
            format_settings,
            metadata_snapshot->getSampleBlock(),
            context,
            getHTTPTimeouts(context),
            compression_method,
            http_method);
    }
}

SchemaCache & IStorageURLBase::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_url", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> IStorageURLBase::tryGetColumnsFromCache(
    const Strings & urls,
    const HTTPHeaderEntries & headers,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    auto & schema_cache = getSchemaCache(context);
    for (const auto & url : urls)
    {
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            auto last_mod_time = getLastModificationTime(url, headers, credentials, context);
            /// Some URLs could not have Last-Modified header, in this case we cannot be sure that
            /// data wasn't changed after adding it's schema to cache. Use schema from cache only if
            /// special setting for this case is enabled.
            if (!last_mod_time && !context->getSettingsRef().schema_inference_cache_require_modification_time_for_url)
                return 0;
            return last_mod_time;
        };

        auto cache_key = getKeyForSchemaCache(url, format_name, format_settings, context);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void IStorageURLBase::addColumnsToCache(
    const Strings & urls,
    const ColumnsDescription & columns,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    auto & schema_cache = getSchemaCache(context);
    auto cache_keys = getKeysForSchemaCache(urls, format_name, format_settings, context);
    schema_cache.addMany(cache_keys, columns);
}

std::optional<time_t> IStorageURLBase::getLastModificationTime(
    const String & url,
    const HTTPHeaderEntries & headers,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const ContextPtr & context)
{
    try
    {
        const auto & settings = context->getSettingsRef();

        auto uri = Poco::URI(url);

        auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withSettings(context->getReadSettings())
                   .withTimeouts(getHTTPTimeouts(context))
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withBufSize(settings.max_read_buffer_size)
                   .withRedirects(settings.max_http_get_redirects)
                   .withHeaders(headers)
                   .create(credentials);

        return buf->tryGetLastModificationTime();
    }
    catch (...)
    {
        return std::nullopt;
    }
}

StorageURL::StorageURL(
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const String & compression_method_,
    const HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_)
    : IStorageURLBase(
        uri_,
        context_,
        table_id_,
        format_name_,
        format_settings_,
        columns_,
        constraints_,
        comment,
        compression_method_,
        headers_,
        http_method_,
        partition_by_)
{
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri));
}


StorageURLWithFailover::StorageURLWithFailover(
    const std::vector<String> & uri_options_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    const String & compression_method_)
    : StorageURL("", table_id_, format_name_, format_settings_, columns_, constraints_, String{}, context_, compression_method_)
{
    for (const auto & uri_option : uri_options_)
    {
        Poco::URI poco_uri(uri_option);
        context_->getRemoteHostFilter().checkURL(poco_uri);
        LOG_DEBUG(getLogger("StorageURLDistributed"), "Adding URL option: {}", uri_option);
        uri_options.emplace_back(uri_option);
    }
}

StorageURLSink::~StorageURLSink()
{
    if (isCancelled())
        cancelBuffers();
}

FormatSettings StorageURL::getFormatSettingsFromArgs(const StorageFactory::Arguments & args)
{
    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    FormatSettings format_settings;
    if (args.storage_def->settings)
    {
        FormatFactorySettings user_format_settings;

        // Apply changed settings from global context, but ignore the
        // unknown ones, because we only have the format settings here.
        const auto & changes = args.getContext()->getSettingsRef().changes();
        for (const auto & change : changes)
        {
            if (user_format_settings.has(change.name))
            {
                user_format_settings.set(change.name, change.value);
            }
        }

        // Apply changes from SETTINGS clause, with validation.
        user_format_settings.applyChanges(args.storage_def->settings->changes);

        format_settings = getFormatSettings(args.getContext(), user_format_settings);
    }
    else
    {
        format_settings = getFormatSettings(args.getContext());
    }

    return format_settings;
}

ASTs::iterator StorageURL::collectHeaders(
    ASTs & url_function_args, HTTPHeaderEntries & header_entries, ContextPtr context)
{
    ASTs::iterator headers_it = url_function_args.end();

    for (auto arg_it = url_function_args.begin(); arg_it != url_function_args.end(); ++arg_it)
    {
        const auto * headers_ast_function = (*arg_it)->as<ASTFunction>();
        if (headers_ast_function && headers_ast_function->name == "headers")
        {
            if (headers_it != url_function_args.end())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "URL table function can have only one key-value argument: headers=(). {}",
                    bad_arguments_error_message);

            const auto * headers_function_args_expr = assert_cast<const ASTExpressionList *>(headers_ast_function->arguments.get());
            auto headers_function_args = headers_function_args_expr->children;

            for (auto & header_arg : headers_function_args)
            {
                const auto * header_ast = header_arg->as<ASTFunction>();
                if (!header_ast || header_ast->name != "equals")
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Headers argument is incorrect. {}", bad_arguments_error_message);

                const auto * header_args_expr = assert_cast<const ASTExpressionList *>(header_ast->arguments.get());
                auto header_args = header_args_expr->children;
                if (header_args.size() != 2)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Headers argument is incorrect: expected 2 arguments, got {}",
                            header_args.size());

                auto ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[0], context);
                auto arg_name_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_name_value.getType() != Field::Types::Which::String)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header name");
                auto arg_name = arg_name_value.safeGet<String>();

                ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[1], context);
                auto arg_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_value.getType() != Field::Types::Which::String)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header value");

                header_entries.emplace_back(arg_name, arg_value.safeGet<String>());
            }

            headers_it = arg_it;

            continue;
        }

        if (headers_ast_function && headers_ast_function->name == "equals")
            continue;

        (*arg_it) = evaluateConstantExpressionOrIdentifierAsLiteral((*arg_it), context);
    }

    return headers_it;
}

void StorageURL::processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection)
{
    /// Headers in config file will have structure "headers.header.name" and "headers.header.value".
    /// But Poco::AbstractConfiguration converts them into "header", "header[1]", "header[2]".
    static const std::vector<std::shared_ptr<re2::RE2>> optional_regex_keys
    {
        std::make_shared<re2::RE2>(R"(headers.header\[[0-9]*\].name)"),
        std::make_shared<re2::RE2>(R"(headers.header\[[0-9]*\].value)"),
    };

    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys, optional_regex_keys);

    configuration.url = collection.get<String>("url");
    configuration.headers = getHeadersFromNamedCollection(collection);

    configuration.http_method = collection.getOrDefault<String>("http_method", collection.getOrDefault<String>("method", ""));
    if (!configuration.http_method.empty() && configuration.http_method != Poco::Net::HTTPRequest::HTTP_POST
        && configuration.http_method != Poco::Net::HTTPRequest::HTTP_PUT)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "HTTP method can be POST or PUT (current: {}). For insert default is POST, for select GET",
            configuration.http_method);

    configuration.format = collection.getOrDefault<String>("format", "auto");
    configuration.compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    configuration.structure = collection.getOrDefault<String>("structure", "auto");
}

StorageURL::Configuration StorageURL::getConfiguration(ASTs & args, const ContextPtr & local_context)
{
    StorageURL::Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, local_context))
    {
        StorageURL::processNamedCollectionResult(configuration, *named_collection);
        collectHeaders(args, configuration.headers, local_context);
    }
    else
    {
        if (args.empty() || args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, bad_arguments_error_message);

        auto header_it = collectHeaders(args, configuration.headers, local_context);
        if (header_it != args.end())
            args.erase(header_it);

        configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");
        if (args.size() > 1)
            configuration.format = checkAndGetLiteralArgument<String>(args[1], "format");
        if (args.size() == 3)
            configuration.compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(Poco::URI(configuration.url).getPath(), true);

    for (const auto & [header, value] : configuration.headers)
    {
        if (header == "Range")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range headers are not allowed");
    }

    return configuration;
}


void registerStorageURL(StorageFactory & factory)
{
    factory.registerStorage(
        "URL",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;
            auto configuration = StorageURL::getConfiguration(engine_args, args.getLocalContext());
            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

            ASTPtr partition_by;
            if (args.storage_def->partition_by)
                partition_by = args.storage_def->partition_by->clone();

            return StorageURL::create(
                configuration.url,
                args.table_id,
                configuration.format,
                format_settings,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                configuration.compression_method,
                configuration.headers,
                configuration.http_method,
                partition_by);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::URL,
        });
}

}
