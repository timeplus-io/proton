#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/getObjectInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>

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
    using ObjectInfos = std::unordered_map<String, DB::S3::ObjectInfo>;
    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfo next() = 0;
        virtual size_t getTotalSize() const = 0;

        KeyWithInfo operator()() { return next(); }
    };

    class KeysIterator : public IIterator
    {
    public:
        explicit KeysIterator(
            const std::shared_ptr<const DB::S3::Client> & client_, /// proton: updated
            const std::string & version_id_,
            const std::vector<String> & keys_,
            const String & bucket_,
            const S3Settings::RequestSettings & request_settings_,
            ASTPtr query,
            const Block & virtual_header,
            ContextPtr context,
            ObjectInfos * object_infos = nullptr);

        KeyWithInfo next() override;
        size_t getTotalSize() const override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class ReadTaskIterator : public IIterator
    {
    public:
        explicit ReadTaskIterator(const ReadTaskCallback & callback_) : callback(callback_) { }

        KeyWithInfo next() override { return {callback(), {}}; }

        size_t getTotalSize() const override { return 0; }

    private:
        ReadTaskCallback callback;
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
        const S3Settings::RequestSettings & request_settings_,
        String compression_hint_,
        const std::shared_ptr<const DB::S3::Client> & client_,
        const String & bucket,
        const String & version_id,
        std::shared_ptr<IIterator> file_iterator_,
        size_t download_thread_num);

    ~IcebergSource() override;

    String getName() const override;

    Chunk generate() override;

    void onCancel() override;

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
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : path(std::move(path_)), read_buf(std::move(read_buf_)), pipeline(std::move(pipeline_)), reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getPath() const { return path; }

    private:
        String path;
        std::unique_ptr<ReadBuffer> read_buf;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    struct ReadBufferOrFactory
    {
        std::unique_ptr<ReadBuffer> buf;
        SeekableReadBufferFactoryPtr buf_factory;
    };

    ReaderHolder reader;

    /// onCancel and generate can be called concurrently
    std::mutex reader_mutex;
    std::vector<NameAndTypePair> requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    size_t download_thread_num = 1;

    Poco::Logger * log = &Poco::Logger::get("IcebergSource");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    UInt64 total_rows_approx_max = 0;
    size_t total_rows_count_times = 0;
    UInt64 total_rows_approx_accumulated = 0;

    /// Recreate ReadBuffer and BlockInputStream for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    ReadBufferOrFactory createS3ReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size);
};

}

}

#endif
