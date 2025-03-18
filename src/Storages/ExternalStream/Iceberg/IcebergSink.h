#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>
#include <Storages/Iceberg/ICatalog.h>
#include <Storages/Iceberg/Manifest.h>
#include <Storages/Iceberg/ManifestList.h>

namespace DB
{

namespace ExternalStream
{

class IcebergSink final : public SinkToStorage
{
public:
    IcebergSink(
        const StorageID & storage_id_,
        const String & format_,
        const Block & sample_block_,
        std::optional<FormatSettings> format_settings_,
        const IcebergS3Configuration & s3_configuration_,
        const String & bucket_,
        const String & key_,
        UInt64 min_upload_file_size_,
        UInt64 max_upload_idle_seconds_,
        Apache::Iceberg::TableMetadata metadata_,
        std::list<Apache::Iceberg::ManifestList> current_manifest_lists,
        const Apache::Iceberg::CatalogPtr & catalog_,
        ContextPtr context_);

    ~IcebergSink() override;

    String getName() const override { return "IcebergSink"; }

    void consume(Chunk chunk) override;
    void onCancel() override;
    void onException() override;
    void onFinish() override;

private:
    void generateManifestList(const UUID & commit_uuid, int64_t snapshot_id, uint64_t sequence_number, size_t attempts);
    void writeManifestList() const;

    void generateManifest(const UUID & commit_uuid);
    void writeManifest() const;

    void commit();

    void finalize();

    const StorageID storage_id;
    const String format;
    const Block sample_block;

    Apache::Iceberg::CatalogPtr catalog;
    Apache::Iceberg::TableMetadata metadata;
    std::list<Apache::Iceberg::ManifestList> manifest_lists;

    const IcebergS3Configuration s3_configuration;
    String bucket;
    [[maybe_unused]] const String key;
    std::optional<FormatSettings> format_settings;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;

    bool cancelled = false;
    std::mutex cancel_mutex;

    size_t current_total_size{0};
    UInt64 min_upload_file_size{0};
    UInt64 max_upload_idle_seconds{0};

    ASTPtr file_exprssion_ast;

    bool stopped = false;
    Stopwatch upload_idle_timer;

    std::function<void(size_t, size_t)> next_callback;

    String current_data_file_uri;
    String current_manifest_list_uri;
    String current_manifest_uri;

    Apache::Iceberg::Manifest current_manifest;

    ContextPtr context;
    Poco::Logger * logger;
};

}

}

#endif
