#include <Storages/ExternalStream/Iceberg/IcebergSource.h>

#if USE_AWS_S3

#include <DataTypes/DataTypesNumber.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromS3.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ReadFromStorageProgress.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/parseGlobs.h>

#include <re2/re2.h>

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
}

namespace ExternalStream
{

namespace
{

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

class IcebergSource::KeysIterator::Impl : WithContext
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
        ObjectInfos * object_infos_)
        : WithContext(context_), bucket(bucket_), query(query_), virtual_header(virtual_header_)
    {
        Strings all_keys = keys_;

        /// Create a virtual block with one row to construct filter
        if (query && virtual_header && !all_keys.empty())
        {
            /// Append "idx" column as the filter result
            virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

            auto block = virtual_header.cloneEmpty();
            addPathToVirtualColumns(block, fs::path(bucket) / all_keys.front(), 0);

            ASTPtr filter_ast;
            VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);

            if (filter_ast)
            {
                block = virtual_header.cloneEmpty();
                for (size_t i = 0; i < all_keys.size(); ++i)
                    addPathToVirtualColumns(block, fs::path(bucket) / all_keys[i], i);

                VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
                const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

                Strings filtered_keys;
                filtered_keys.reserve(block.rows());
                for (UInt64 idx : idxs.getData())
                    filtered_keys.emplace_back(std::move(all_keys[idx]));

                all_keys = std::move(filtered_keys);
            }
        }

        for (auto && key : all_keys)
        {
            std::optional<DB::S3::ObjectInfo> info;

            /// To avoid extra requests update total_size only if object_infos != nullptr
            /// (which means we eventually need this info anyway, so it should be ok to do it now)
            if (object_infos_ != nullptr)
            {
                info = DB::S3::getObjectInfo(*client_, bucket, key, version_id_, request_settings_);
                total_size += info->size;

                String path = fs::path(bucket) / key;
                (*object_infos_)[std::move(path)] = *info;
            }

            keys.emplace_back(std::move(key), std::move(info));
        }
    }

    KeyWithInfo next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};

        return keys[current_index];
    }

    size_t getTotalSize() const { return total_size; }

private:
    KeysWithInfo keys;
    std::atomic_size_t index = 0;

    String bucket;
    ASTPtr query;
    Block virtual_header;

    size_t total_size = 0;
};

IcebergSource::KeysIterator::KeysIterator(
    const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
    const std::string & version_id_,
    const std::vector<String> & keys_,
    const String & bucket_,
    const S3Settings::RequestSettings & request_settings_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    ObjectInfos * object_infos)
    : pimpl(std::make_shared<IcebergSource::KeysIterator::Impl>(
          client_, version_id_, keys_, bucket_, request_settings_, query, virtual_header, context, object_infos))
{
}

IcebergSource::KeyWithInfo IcebergSource::KeysIterator::next()
{
    return pimpl->next();
}

size_t IcebergSource::KeysIterator::getTotalSize() const
{
    return pimpl->getTotalSize();
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
    const S3Settings::RequestSettings & request_settings_,
    const String compression_hint_,
    const std::shared_ptr<const DB::S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_), true, ProcessorID::IcebergSourceID)
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


void IcebergSource::onCancel()
{
    std::lock_guard lock(reader_mutex);
    if (reader)
        reader->cancel();
}


IcebergSource::ReaderHolder IcebergSource::createReader()
{
    auto [current_key, info] = (*file_iterator)();
    if (current_key.empty())
        return {};

    size_t object_size = info ? info->size : DB::S3::getObjectSize(*client, bucket, current_key, version_id, request_settings);

    auto compression_method = chooseCompressionMethod(current_key, compression_hint);

    InputFormatPtr input_format;
    std::unique_ptr<ReadBuffer> owned_read_buf;

    auto read_buf_or_factory = createS3ReadBuffer(current_key, object_size);
    if (read_buf_or_factory.buf_factory)
    {
        input_format = FormatFactory::instance().getInputRandomAccess(
            format,
            std::move(read_buf_or_factory.buf_factory),
            sample_block,
            getContext(),
            max_block_size,
            /* is_remote_fs */ true,
            compression_method,
            format_settings);
    }
    else
    {
        owned_read_buf = wrapReadBufferWithCompressionMethod(
            std::move(read_buf_or_factory.buf), compression_method, static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));
        input_format
            = FormatFactory::instance().getInput(format, *owned_read_buf, sample_block, getContext(), max_block_size, format_settings);
    }

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

    return ReaderHolder{fs::path(bucket) / current_key, std::move(owned_read_buf), std::move(pipeline), std::move(current_reader)};
}

std::future<IcebergSource::ReaderHolder> IcebergSource::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, Priority{});
}

IcebergSource::ReadBufferOrFactory IcebergSource::createS3ReadBuffer(const String & key, size_t object_size)
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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
        return {.buf = createAsyncS3ReadBuffer(key, read_settings, object_size)};
#pragma clang diagnostic pop
    }

    auto factory = std::make_unique<ReadBufferS3Factory>(client, bucket, key, version_id, object_size, request_settings, read_settings);
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
    return {.buf_factory = std::move(factory)};
#pragma clang diagnostic pop
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
        StoredObjects{StoredObject{key, object_size}},
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

            const auto & file_path = reader.getPath();
            size_t total_size = file_iterator->getTotalSize();
            if ((num_rows != 0u) && (total_size != 0u))
            {
                updateRowsProgressApprox(
                    *this, chunk, total_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
            }

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
