#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Databases/ApacheIceberg/ApacheIcebergStorageType.h>
#include <Storages/Iceberg/Requirement.h>
#include <Storages/Iceberg/StorageCredentials.h>
#include <Storages/Iceberg/Update.h>

namespace Apache::Iceberg
{
using StorageType = DB::ApacheIcebergStorageType;
StorageType parseStorageTypeFromLocation(const std::string & location);

/// A class representing table metadata,
/// which was received from Catalog.
/*
{
  "config": {
    "metadata_location": "s3://tp-gimi-test/hello_world/metadata/00001-9aebf755-36b2-4838-86c5-ff550dab047c.metadata.json",
    "metadata_hashcode": "4daa63078f8baa111e03f08ec4c68b74d73fede391c5c689f76362e4893c357b",
    "iceberg.table.uuid": "95711216-532a-441d-8973-11dcfb96d3c1",
    "iceberg.table.lastUpdatedMs": "1739341126355",
    "write.parquet.compression-codec": "zstd",
    "table_type": "iceberg"
  },
  "metadata": {
    "current-schema-id": 0,
    "current-snapshot-id": 704111412443037022,
    "default-sort-order-id": 0,
    "default-spec-id": 0,
    "format-version": 2,
    "last-column-id": 3,
    "last-partition-id": 999,
    "last-sequence-number": 1,
    "last-updated-ms": 1739341126355,
    "location": "s3://tp-gimi-test/hello_world",
    "metadata-log": [],
    "partition-specs": [
      {
        "fields": [],
        "spec-id": 0
      }
    ],
    "partition-statistics-files": [],
    "properties": {
      "write.parquet.compression-codec": "zstd"
    },
    "refs": {
      "main": {
        "snapshot-id": 704111412443037022,
        "type": "branch"
      }
    },
    "schemas": [
      {
        "fields": [
          {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "int"
          },
          {
            "id": 2,
            "name": "key",
            "required": true,
            "type": "string"
          },
          {
            "id": 3,
            "name": "value",
            "required": true,
            "type": "double"
          }
        ],
        "schema-id": 0,
        "type": "struct"
      }
    ],
    "snapshot-log": [
      {
        "snapshot-id": 704111412443037022,
        "timestamp-ms": 1739341126355
      }
    ],
    "snapshots": [
      {
        "manifest-list": "s3://tp-gimi-test/hello_world/metadata/snap-704111412443037022-1-00d110f2-5cbe-4802-a2d4-a2e1e8b0cdaf.avro",
        "schema-id": 0,
        "sequence-number": 1,
        "snapshot-id": 704111412443037022,
        "summary": {
          "engine-version": "3.5.4",
          "added-data-files": "1",
          "total-equality-deletes": "0",
          "app-id": "local-1739341024141",
          "added-records": "1",
          "total-records": "1",
          "spark.app.id": "local-1739341024141",
          "changed-partition-count": "1",
          "engine-name": "spark",
          "total-position-deletes": "0",
          "added-files-size": "843",
          "total-delete-files": "0",
          "iceberg-version": "Apache Iceberg 1.7.1 (commit 4a432839233f2343a9eae8255532f911f06358ef)",
          "total-files-size": "843",
          "total-data-files": "1",
          "operation": "append"
        },
        "timestamp-ms": 1739341126355
      }
    ],
    "sort-orders": [
      {
        "fields": [],
        "order-id": 0
      }
    ],
    "statistics-files": [],
    "table-uuid": "95711216-532a-441d-8973-11dcfb96d3c1"
  },
  "metadata-location": "s3://tp-gimi-test/hello_world/metadata/00001-9aebf755-36b2-4838-86c5-ff550dab047c.metadata.json"
}
*/
class TableMetadata
{
public:
    TableMetadata() = default;

    TableMetadata & withLocation()
    {
        with_location = true;
        return *this;
    }
    TableMetadata & withSchema()
    {
        with_schema = true;
        return *this;
    }
    TableMetadata & withStorageCredentials()
    {
        with_storage_credentials = true;
        return *this;
    }

    void setLocation(const std::string & location_);
    std::string getLocation(bool path_only) const;

    void setSchema(const DB::NamesAndTypesList & schema_);
    const DB::NamesAndTypesList & getSchema() const;

    void setSchemaJSON(const std::string & schema_json_);
    const std::string & getSchemaJSON() const;

    void setCurrentSchemaID(int64_t current_schema_id_);
    int64_t getCurrentSchemaID() const;

    void setStorageCredentials(std::shared_ptr<IStorageCredentials> credentials_);
    std::shared_ptr<IStorageCredentials> getStorageCredentials() const;

    int64_t getSnapshotID() const { return snapshot_id; }
    void setSnapshotID(int64_t id) { snapshot_id = id; }

    uint64_t getSequenceNumber() const { return sequence_number; }
    void setSequenceNumber(uint64_t sn) { sequence_number = sn; }

    const std::string & getTableUUID() const { return table_uuid; }
    void setTableUUID(std::string uuid) { table_uuid = uuid; }

    int getFormatVersion() const { return format_version; }
    void setFormatVersion(int version) { format_version = version; }

    const std::string & getManifestList() const { return current_snapshot.manifest_list; }

    const Snapshot & getCurrentSnapshot() const { return current_snapshot; }
    void setCurrentSnapshot(Snapshot && snapshot_) { current_snapshot = std::move(snapshot_); }

    bool requiresLocation() const { return with_location; }
    bool requiresSchema() const { return with_schema; }
    bool requiresCredentials() const { return with_storage_credentials; }

    StorageType getStorageType() const;

private:
    /// Starts with s3://, file://, etc.
    /// For example, `s3://bucket/`
    std::string location_without_path;
    /// Path to table's data: `/path/to/table/data/`
    std::string path;
    DB::NamesAndTypesList schema;
    std::string iceberg_schema;
    int64_t current_schema_id{0};

    int64_t snapshot_id{0};
    uint64_t sequence_number{0};

    std::string table_uuid;
    int format_version{0};
    Snapshot current_snapshot;

    /// Storage credentials, which are called "vended credentials".
    std::shared_ptr<IStorageCredentials> storage_credentials;

    bool with_location = false;
    bool with_schema = false;
    bool with_storage_credentials = false;
};


/// Base class for catalog implementation.
/// Used for communication with the catalog.
class ICatalog
{
public:
    using Namespaces = std::vector<std::string>;

    explicit ICatalog(const std::string & warehouse_) : warehouse(warehouse_) { }

    virtual ~ICatalog() = default;

    const std::string & getWarehouse() const { return warehouse; }

    virtual bool existsNamespace(const std::string & name) const = 0;
    virtual void createNamespace(const std::string & name) const = 0;

    /// Does catalog namespace have any tables?
    virtual bool empty(const std::string & namespace_name) const = 0;

    /// Fetch tables' names list.
    /// Contains full namespaces in names.
    virtual DB::Names getTables(const std::string & namespace_name) const = 0;

    /// Check that a table exists in a given namespace.
    virtual bool existsTable(const std::string & namespace_name, const std::string & table_name) const = 0;

    virtual void createTable(
        const std::string & namespace_name,
        const std::string & name,
        std::optional<const std::string> location,
        const DB::NamesAndTypesList & schema, /// TODO define a Schema struct/class.
        std::optional<const std::string> partition_spec,
        std::optional<const std::string> write_order,
        bool stage_create,
        std::unordered_map<std::string, std::string> properties)
        = 0;

    virtual void deleteTable(const std::string & namespace_name, const std::string & name) = 0;

    /// Commits the updates to the target table, and update table_metadata.
    virtual void commitTable(
        const std::string & namespace_name,
        const std::string & name,
        const Requirements & requirements,
        const Updates & updates,
        TableMetadata & table_metadata)
        = 0;

    /// Get table metadata in the given namespace.
    /// Throw exception if table does not exist.
    virtual void getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const = 0;

    /// Get table metadata in the given namespace.
    /// Return `false` if table does not exist, `true` otherwise.
    virtual bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const = 0;

    /// Get storage type, where Iceberg tables' data is stored.
    /// E.g. one of S3, Azure, Local, HDFS.
    virtual std::optional<StorageType> getStorageType() const = 0;

protected:
    /// Name of the warehouse,
    /// which is sometimes also called "catalog name".
    const std::string warehouse;
};

using CatalogPtr = std::shared_ptr<ICatalog>;

}
