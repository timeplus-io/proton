#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{

class ASTStorage;

#define S3_EXTERNAL_TABLE_SETTINGS(M, ALIAS) \
    M(String, endpoint, "", "Configure an optional custom endpoint for the S3 API.", 0) \
    M(String, region, "", "The region in where the bucket is.", 0) \
    M(String, bucket, "", "The bucket name.", 0) \
    M(String, read_from, "", "Configure the object key to read for SELECT queries, supports wildcards", 0) \
    M(String, write_to, "", "Configure the object key to write for INSERT queries.", 0) \
    M(String, access_key_id, "", "The access key ID.", 0) \
    M(String, secret_access_key, "", "The secret access key.", 0) \
    M(String, \
      compression_method, \
      "auto", \
      "Supported values: auto, none, gzip, deflate, br, xz, zstd, lz4, bz2, and snappy. By default, it will autodetect compression " \
      "method by file extension from the object keys.", \
      0) \
    M(Bool, use_environment_credentials, false, "", 0)

#define DBMS_EXTERNAL_TABLE_SETTINGS(M, ALIAS) \
    M(String, address, "", "The address of the database server to connect", 0) \
    M(String, user, "default", "The user to be used to connect to the database server", 0) \
    M(String, password, "", "The password to be used to connect to the database server", 0) \
    M(Bool, secure, false, "Indicates if it uses secure connection", 0) \
    M(Bool, compression, true, "Indicates if compression should be enabled", 0) \
    M(String, ssl_verify_mode, "relaxed", "SSL certificate verification mode: none, relaxed, strict, once", 0) \
    M(String, ssl_ca_cert_file, "", "Path to SSL CA certificate file", 0) \
    M(String, ssl_cert_file, "", "Path to SSL certificate file", 0) \
    M(String, ssl_key_file, "", "Path to SSL private key file", 0) \
    M(String, database, "default", "The datababse to connect to", 0) \
    M(String, table, "", "The remote database table to which the external table is mapped", 0) \
    M(UInt64, \
      pooled_connections, \
      std::numeric_limits<UInt64>::max(), \
      "Max pooled TCP connections to the database server. If not set, each external table type will pick its default value", \
      0) \
    /** PostgreSQL */ \
    M(String, schema, "", "PostgreSQL non-default schema", 0) \
    M(String, on_conflict, "", "PostgreSQL conflict resolution strategy", 0) \
    /** MongoDB */ \
    M(String, uri, "", "MongoDB server's connection URI", 0) \
    M(String, collection, "", "Remote collection name", 0) \
    M(String, connection_options, "", "MongoDB connection string options as a URL formatted string. e.g. 'authSource=admin&ssl=true'", 0) \
    M(String, oid_columns, "_id", "Comma-separated list of columns that should be treated as oid in the WHERE clause. _id by default", 0) \
    /** MySQL */ \
    M(Bool, replace_query, false, "Flag that converts 'INSERT INTO' queries to 'REPLACE INTO'", 0) \
    M(String, on_duplicate_clause, "", "The 'ON DUPLICATE KEY on_duplicate_clause' expression that is added to the 'INSERT' query.", 0)

#define LIST_OF_EXTERNAL_TABLE_SETTINGS(M, ALIAS) \
    M(String, type, "", "External table type", 0) \
    M(String, \
      data_format, \
      "", \
      "The format of the data stored in the external table, for example JSONEachRow, not supported by all kinds of external tables.", \
      0) \
    M(String, config_file, "", "External table configuration file path", 0) \
    DBMS_EXTERNAL_TABLE_SETTINGS(M, ALIAS) \
    S3_EXTERNAL_TABLE_SETTINGS(M, ALIAS) \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(ExternalTableSettingsTraits, LIST_OF_EXTERNAL_TABLE_SETTINGS)


/// Settings for the ExternalTable engine.
/// Could be loaded from a CREATE EXTERNAL TABLE query (SETTINGS clause).
struct ExternalTableSettings : public BaseSettings<ExternalTableSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

using ExternalTableSettingsPtr = std::unique_ptr<ExternalTableSettings>;

}
