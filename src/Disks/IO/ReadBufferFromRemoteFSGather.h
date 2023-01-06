#pragma once

#include "config.h"
#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>

#if USE_AZURE_BLOB_STORAGE
#include <azure/storage/blobs.hpp>
#endif

namespace Aws { namespace S3 { class S3Client; } }

namespace Poco { class Logger; }

namespace DB
{

/**
 * Remote disk might need to split one clickhouse file into multiple files in remote fs.
 * This class works like a proxy to allow transition from one file into multiple.
 */
class ReadBufferFromRemoteFSGather : public ReadBuffer
{
friend class ReadIndirectBufferFromRemoteFS;

public:
    ReadBufferFromRemoteFSGather(
        BlobsPathWithSize blobs_to_read_,
        const ReadSettings & settings_,
        const String & path_);

    String getFileName() const;

    void reset();

    void setReadUntilPosition(size_t position) override;

    struct ReadResult
    {
        size_t size = 0;
        size_t offset = 0;
    };

    ReadResult readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

    size_t getFileSize() const;

    size_t getFileOffsetOfBufferEnd() const;

    bool initialized() const { return current_buf != nullptr; }

    String getInfoForLog();

    size_t getImplementationBufferOffset() const;

protected:
    virtual SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size) = 0;

    BlobsPathWithSize blobs_to_read;

    ReadSettings settings;

    bool use_external_buffer;

    size_t read_until_position = 0;

    String current_path;

    String canonical_path;

private:
    bool nextImpl() override;

    void initialize();

    bool readImpl();

    bool moveToNextBuffer();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t file_offset_of_buffer_end = 0;

    /**
     * File:                        |___________________|
     * Buffer:                            |~~~~~~~|
     * file_offset_of_buffer_end:                 ^
     */
    size_t bytes_to_ignore = 0;


    Poco::Logger * log;
};


#if USE_AWS_S3
/// Reads data from S3 using stored paths in metadata.
class ReadBufferFromS3Gather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromS3Gather(
        const String & path_,
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        BlobsPathWithSize blobs_to_read_,
        size_t max_single_read_retries_,
        const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(std::move(blobs_to_read_), settings_, path_)
        , client_ptr(std::move(client_ptr_))
        , bucket(bucket_)
        , max_single_read_retries(max_single_read_retries_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size) override;

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    UInt64 max_single_read_retries;
};
#endif


#if USE_AZURE_BLOB_STORAGE
/// Reads data from AzureBlob Storage using paths stored in metadata.
class ReadBufferFromAzureBlobStorageGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromAzureBlobStorageGather(
        const String & path_,
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        BlobsPathWithSize blobs_to_read_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(std::move(blobs_to_read_), settings_, path_)
        , blob_container_client(blob_container_client_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size) override;

private:
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
};
#endif


class ReadBufferFromWebServerGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromWebServerGather(
            const String & path_,
            const String & uri_,
            BlobsPathWithSize blobs_to_read_,
            ContextPtr context_,
            const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(std::move(blobs_to_read_), settings_, path_)
        , uri(uri_)
        , context(context_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size) override;

private:
    String uri;
    ContextPtr context;
};
}
