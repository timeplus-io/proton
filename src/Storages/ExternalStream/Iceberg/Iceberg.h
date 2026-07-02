#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>
#include <Storages/ExternalStream/Iceberg/IcebergSource.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/Iceberg/ICatalog.h>
#include <Storages/Iceberg/Manifest.h>
#include <Storages/Iceberg/ManifestList.h>
#include <Storages/StorageS3Settings.h>

namespace DB::ExternalStream
{

class Iceberg final : public StorageExternalStreamImpl
{
public:
    Iceberg(StorageID, StorageInMemoryMetadata, ExternalStreamSettingsPtr, ExternalStreamCounterPtr, ContextPtr);
    ~Iceberg() override = default;

    void validateSettings(const ExternalStreamSettingsPtr &, bool, const ContextPtr &) const override { }

    String getName() const override { return "IcebergExternalStream"; }

    /// For now, we only support Parquet format and it supports subset of columns.
    bool supportsSubsetOfColumns(const ContextPtr &) const { return true; }

    /// Streaming query has not implemented yet. Will switch back to `true` once it's implemented.
    bool supportsStreamingQuery() const override { return false; }

    /// FIXME: Supports parallel insert
    bool supportsParallelInsert() const override { return false; }

    std::optional<UInt64> totalRows(const Settings &) const override;

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
    static std::list<Apache::Iceberg::ManifestList>
    fetchManifestList(const Apache::Iceberg::TableMetadata &, const IcebergS3Configuration &, LoggerPtr logger_);

private:
    void prepareS3Configuration(const ContextPtr &);
    void calculateVirtualColumns();

    Apache::Iceberg::CatalogPtr getCatalog() const;
    Apache::Iceberg::TableMetadata getTableMetadata() const;
    Apache::Iceberg::TableMetadata tryGetTableMetadata() const;

    FormatSettings getFormatSettings(const ContextPtr & local_context) const;

    NamesAndTypesList virtual_columns;
    Block virtual_block;

    IcebergS3Configuration s3_configuration;
    std::optional<FormatFactorySettings> format_factory_settings;
    std::string iceberg_schema_json;
};

}

#endif
