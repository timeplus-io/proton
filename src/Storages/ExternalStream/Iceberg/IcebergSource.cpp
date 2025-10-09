#include <Storages/ExternalStream/Iceberg/IcebergSource.h>

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <DataTypes/DataTypesNumber.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Formats/Avro/InputStreamReadBufferAdapter.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3Common.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Iceberg/AvroSchemas.h>
#include <Storages/Iceberg/Manifest.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <boost/algorithm/string/predicate.hpp>
#include <Compiler.hh>
#include <DataFile.hh>

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
extern const int UNEXPECTED_EXPRESSION;
extern const int ICEBERG_CATALOG_ERROR;
}

namespace ExternalStream
{

class IcebergSource::KeysIterator::Impl
{
public:
    explicit Impl(
        std::list<Apache::Iceberg::ManifestList> manifest_list_,
        const IcebergS3Configuration & s3_configuration,
        std::function<void(FileProgress)> file_progress_callback_,
        LoggerPtr logger_)
        : manifest_lists(manifest_list_)
        , s3_client(s3_configuration.client)
        , request_settings(s3_configuration.request_settings)
        , file_progress_callback(file_progress_callback_)
        , logger(logger_)
    {
        next_manifest_list = manifest_lists.begin();
        next_data_file = current_data_files.begin();
    }

    KeyWithInfo next()
    {
        std::lock_guard lock(next_mutex);
        return nextAssumeLocked();
    }

private:
    KeyWithInfo nextAssumeLocked()
    {
        if (next_data_file == current_data_files.end())
        {
            if (next_manifest_list == manifest_lists.end())
                return {};

            std::list<String> data_files;
            fetchDataFiles(*next_manifest_list, data_files);
            ++next_manifest_list;

            current_data_files.swap(data_files);
            next_data_file = current_data_files.begin();
            return nextAssumeLocked();
        }

        auto data_object = S3::URI(*next_data_file);
        ++next_data_file;

        std::optional<S3::ObjectInfo> info;
        if (file_progress_callback)
        {
            info = S3::getObjectInfo(*s3_client, data_object.bucket, data_object.key, data_object.version_id, request_settings);
            file_progress_callback(FileProgress(0, info->size));
        }

        return {data_object.key, info};
    }

    void fetchDataFiles(const Apache::Iceberg::ManifestList & manifest_list, std::list<String> & data_files) const
    {
        LOG_INFO(logger, "fetching data files from manifest {}", manifest_list.manifest_path);

        auto manifest_object = S3::URI(manifest_list.manifest_path);
        ReadBufferFromS3 buf{
            s3_client,
            manifest_object.bucket,
            manifest_object.key,
            manifest_object.version_id,
            request_settings,
            /*settings_=*/{}};

        auto is = std::make_unique<Avro::InputStreamReadBufferAdapter>(buf);

        auto schema = avro::compileJsonSchemaFromString(Apache::Iceberg::AvroSchemas::MANIFEST);
        avro::DataFileReader<Apache::Iceberg::Manifest> manifest_reader{std::move(is), schema};

        Apache::Iceberg::Manifest manifest;
        auto enc = avro::jsonEncoder(schema);
        while (manifest_reader.read(manifest))
        {
            if (manifest.data_file.content != Apache::Iceberg::DataFileContent::Data)
            {
                LOG_INFO(logger, "Skipped data_file content={} path={}", manifest.data_file.content, manifest.data_file.file_path);
                continue;
            }

            if (!boost::iequals(manifest.data_file.file_format, "parquet"))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Data file {} has unsupported format {}",
                    manifest.data_file.file_path,
                    manifest.data_file.file_format);

            if (manifest.data_file.file_path.empty())
                throw Exception(ErrorCodes::ICEBERG_CATALOG_ERROR, "Data file has empty file_path");

            LOG_INFO(logger, "Found data_file path={}", manifest.data_file.file_path);
            data_files.push_back(std::move(manifest.data_file.file_path));
        }
    }

    std::list<Apache::Iceberg::ManifestList> manifest_lists;
    std::list<Apache::Iceberg::ManifestList>::iterator next_manifest_list;
    std::list<String> current_data_files;
    std::list<String>::iterator next_data_file;

    std::shared_ptr<const S3::Client> s3_client;
    S3Settings::RequestSettings request_settings;
    S3::AuthSettings auth_settings;
    std::function<void(FileProgress)> file_progress_callback;

    std::mutex next_mutex;

    LoggerPtr logger;
};

IcebergSource::KeysIterator::KeysIterator(
    std::list<Apache::Iceberg::ManifestList> manifest_list,
    const IcebergS3Configuration & s3_configuration,
    LoggerPtr logger,
    std::function<void(FileProgress)> file_progress_callback_)
    : pimpl(
          std::make_shared<IcebergSource::KeysIterator::Impl>(std::move(manifest_list), s3_configuration, file_progress_callback_, logger))
{
}

IcebergSource::KeyWithInfo IcebergSource::KeysIterator::next()
{
    return pimpl->next();
}

Block IcebergSource::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

IcebergSource::IcebergSource(
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    const String & format_,
    String name_,
    const Block & sample_block_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const ColumnsDescription & columns_,
    UInt64 max_block_size_,
    const String compression_hint_,
    const IcebergS3Configuration & s3_configuration,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_), true, ProcessorID::StorageS3SourceID)
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(s3_configuration.url.bucket)
    , version_id(s3_configuration.url.version_id)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , request_settings(s3_configuration.request_settings)
    , compression_hint(compression_hint_)
    , client(s3_configuration.client)
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

void IcebergSource::onCancel() noexcept
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
            tryLogCurrentException(log, "Failed to cancel reader");
        }
    }
}


IcebergSource::ReaderHolder IcebergSource::createReader()
{
    auto [current_key, info] = (*file_iterator)();
    if (current_key.empty())
        return {};

    size_t object_size{0};
    if (info)
    {
        object_size = info->size;
    }
    else
    {
        try
        {
            object_size = S3::getObjectSize(*client, bucket, current_key, version_id);
        }
        catch (Exception & e)
        {
            /// If, somehow, the datafile is not available anymore, skip it.
            if (e.message().contains("HTTP response code: 404"))
            {
                LOG_WARNING(log, "Data file {} was not found, skipped", current_key);
                return createReader();
            }

            e.addMessage("Failed to get object size of {}", current_key);
            e.rethrow();
        }
    }

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

std::future<IcebergSource::ReaderHolder> IcebergSource::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, Priority{});
}

std::unique_ptr<ReadBuffer> IcebergSource::createS3ReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool object_too_small = object_size <= 2 * download_buffer_size;

    /// Create a read buffer that will prefetch the first ~1 MB of the file.
    /// When reading lots of tiny files, this prefetching almost doubles the throughput.
    /// For bigger files, parallel reading is more useful.
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
IcebergSource::createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size)
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

IcebergSource::~IcebergSource()
{
    create_reader_pool.wait();
}

String IcebergSource::getName() const
{
    return name;
}

Chunk IcebergSource::generate()
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

}

}
#endif
