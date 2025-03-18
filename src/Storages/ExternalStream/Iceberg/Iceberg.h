#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Storages/Iceberg/ICatalog.h>
#include <Storages/Iceberg/Manifest.h>
#include <Storages/Iceberg/ManifestList.h>
#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>
#include <Storages/ExternalStream/Iceberg/IcebergSource.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/StorageS3Settings.h>

namespace DB
{

namespace ExternalStream
{

class Iceberg final : public StorageExternalStreamImpl
{
    using ObjectInfos = IcebergSource::ObjectInfos;

public:
    Iceberg(IStorage *, ExternalStreamSettingsPtr, ExternalStreamCounterPtr, ContextPtr);
    ~Iceberg() override = default;

    String getName() const override { return "IcebergExternalStream"; }

    /// For now, we only support Parquet format and it supports subset of columns.
    bool supportsSubsetOfColumns() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    /// FIXME this is a temporary trick.
    void setIcebergSchema(const std::string & schema) { iceberg_schema_json = schema; }

private:
    Apache::Iceberg::CatalogPtr getCatalog() const;
    Apache::Iceberg::TableMetadata getTableMetadata() const;
    std::list<Apache::Iceberg::ManifestList> fetchManifestList(const Apache::Iceberg::TableMetadata &) const;
    void fetchDataFiles(const Apache::Iceberg::ManifestList &, std::list<String> & data_files) const;

    FormatSettings getFormatSettings(const ContextPtr & local_context) const;

    NamesAndTypesList virtual_columns;
    Block virtual_block;
    /// Temporary. FIXME
    ObjectInfos object_infos;

    IcebergS3Configuration s3_configuration;
    std::optional<FormatFactorySettings> format_factory_settings;

    std::string iceberg_schema_json;
};

}

}

#endif
