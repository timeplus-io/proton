#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <IO/S3/getObjectInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>
#include <Storages/Iceberg/ManifestList.h>

namespace DB
{

namespace ExternalStream
{

class IcebergSource final : public ISource, WithContext
{
public:
    struct KeyWithInfo
    {
        KeyWithInfo() = default;
        KeyWithInfo(String key_, std::optional<DB::S3::ObjectInfo> info_) : key(std::move(key_)), info(std::move(info_)) { }

        String key;
        std::optional<DB::S3::ObjectInfo> info;
    };

    using KeysWithInfo = std::vector<KeyWithInfo>;

    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfo next() = 0;

        KeyWithInfo operator()() { return next(); }
    };

    class KeysIterator : public IIterator
    {
    public:
        explicit KeysIterator(
            std::list<Apache::Iceberg::ManifestList> manifest_list,
            const IcebergS3Configuration & s3_configuration,
            LoggerPtr logger,
            std::function<void(FileProgress)> progress_callback_ = {});

        KeyWithInfo next() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    IcebergSource(
        const std::vector<NameAndTypePair> & requested_virtual_columns_,
        const String & format,
        String name_,
        const Block & sample_block,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const ColumnsDescription & columns_,
        UInt64 max_block_size_,
        String compression_hint_,
        const IcebergS3Configuration &,
        std::shared_ptr<IIterator> file_iterator_,
        size_t download_thread_num);

    ~IcebergSource() override;

    String getName() const override;

    Chunk generate() override;

    void onCancel() noexcept override;

private:
    String name;
    String bucket;
    String version_id;
    String format;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<const DB::S3::Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            String path_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<IInputFormat> input_format_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : path(std::move(path_))
            , read_buf(std::move(read_buf_))
            , input_format(input_format_)
            , pipeline(std::move(pipeline_))
            , reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getPath() const { return path; }

        const IInputFormat * getInputFormat() const { return input_format.get(); }

    private:
        String path;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<IInputFormat> input_format;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;

    /// onCancel and generate can be called concurrently
    std::mutex reader_mutex;
    std::vector<NameAndTypePair> requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    [[maybe_unused]]size_t download_thread_num = 1;

    LoggerPtr log = getLogger("IcebergSource");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and BlockInputStream for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size);
};
;

}

}

#endif
