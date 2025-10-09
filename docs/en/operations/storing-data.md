---
toc_priority: 68
toc_title: External Disks for Storing Data
---

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely. Various storages are supported:
1. [Amazon S3](https://aws.amazon.com/s3/) object storage.
2. The Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))
3. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

:::note ClickHouse also has support for external table engines, which are different from external storage option described on this page as they allow to read data stored in some general file format (like Parquet), while on this page we are describing storage configuration for ClickHouse `MergeTree` family or `Log` family tables.
1. to work with data stored on `Amazon S3` disks, use [S3](/docs/en/engines/table-engines/integrations/s3.md) table engine.
2. to work with data in the Hadoop Distributed File System — [HDFS](/docs/en/engines/table-engines/integrations/hdfs.md) table engine.
3. to work with data stored in Azure Blob Storage use [AzureBlobStorage](/docs/en/engines/table-engines/integrations/azureBlobStorage.md) table engine.
:::

## Configuring external storage {#configuring-external-storage}

[MergeTree](/docs/en/engines/table-engines/mergetree-family/mergetree.md) and [Log](/docs/en/engines/table-engines/log-family/log.md) family table engines can store data to `S3`, `AzureBlobStorage`, `HDFS` using a disk with types `s3`, `azure_blob_storage`, `hdfs` accordingly.

Disk configuration requires:
1. `type` section, equal to one of `s3`, `azure_blob_storage`, `hdfs`, `local_blob_storage`, `web`.
2. Configuration of a specific external storage type.

Starting from 24.1 clickhouse version, it is possible to use a new configuration option.
It requires to specify:
1. `type` equal to `object_storage`
2. `object_storage_type`, equal to one of `s3`, `azure_blob_storage` (or just `azure` from `24.3`), `hdfs`, `local_blob_storage` (or just `local` from `24.3`), `web`.
Optionally, `metadata_type` can be specified (it is equal to `local` by default), but it can also be set to `plain`, `web` and, starting from `24.4`, `plain_rewritable`.
Usage of `plain` metadata type is described in [plain storage section](/docs/en/operations/storing-data.md/#storing-data-on-webserver), `web` metadata type can be used only with `web` object storage type, `local` metadata type stores metadata files locally (each metadata files contains mapping to files in object storage and some additional meta information about them).

E.g. configuration option
``` xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

is equal to configuration (from `24.1`):
``` xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

Configuration
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

is equal to
``` xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Example of full storage configuration will look like:
``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

Starting with 24.1 clickhouse version, it can also look like:
``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

In order to make a specific kind of storage a default option for all `MergeTree` tables add the following section to configuration file:
``` xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

If you want to configure a specific storage policy only to specific table, you can define it in settings while creating the table:

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

You can also use `disk` instead of `storage_policy`. In this case it is not requires to have `storage_policy` section in configuration file, only `disk` section would be enough.

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

## Dynamic Configuration {#dynamic-configuration}

There is also a possibility to specify storage configuration without a predefined disk in configuration in a configuration file, but can be configured in the `CREATE`/`ATTACH` query settings.

The following example query builds on the above dynamic disk configuration and shows how to use a local disk to cache data from a table stored at a URL.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  # highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  # highlight-end
```

The example below adds cache to external storage.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

In the settings highlighted below notice that the disk of `type=web` is nested within
the disk of `type=cache`.

:::note
The example uses `type=web`, but any disk type can be configured as dynamic, even Local disk. Local disks require a path argument to be inside the server config parameter `custom_local_disks_base_directory`, which has no default, so set that also when using local disk.
:::

A combination of config-based configuration and sql-defined configuration is also possible:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

where `web` is a from a server configuration file:

``` xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely — on [Amazon S3](https://aws.amazon.com/s3/) disks or in the Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

To work with data stored on `Amazon S3` disks use [S3](../engines/table-engines/integrations/s3.md) table engine, and to work with data in the Hadoop Distributed File System — [HDFS](../engines/table-engines/integrations/hdfs.md) table engine.

To load data from a web server with static files use a disk with type [web](#storing-data-on-webserver).

## Zero-copy Replication {#zero-copy}

ClickHouse supports zero-copy replication for `S3` and `HDFS` disks, which means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself.

## Configuring HDFS {#configuring-hdfs}

### Using Plain Storage {#plain-storage}

In `22.10` a new disk type `s3_plain` was introduced, which provides a write-once storage. Configuration parameters are the same as for `s3` disk type.
Unlike `s3` disk type, it stores data as is, e.g. instead of randomly-generated blob names, it uses normal file names (the same way as clickhouse stores files on local disk) and does not store any metadata locally, e.g. it is derived from data on `s3`.

This disk type allows to keep a static version of the table, as it does not allow executing merges on the existing data and does not allow inserting of new data.
A use case for this disk type is to create backups on it, which can be done via `BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. Afterwards you can do `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')` or using `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

Configuration:
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Starting from `24.1` it is possible configure any object storage disk (`s3`, `azure`, `hdfs`, `local`) using `plain` metadata type.

Configuration:
``` xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

### Using S3 Plain Rewritable Storage {#s3-plain-rewritable-storage}
A new disk type `s3_plain_rewritable` was introduced in `24.4`.
Similar to the `s3_plain` disk type, it does not require additional storage for metadata files; instead, metadata is stored in S3.
Unlike `s3_plain` disk type, `s3_plain_rewritable` allows executing merges and supports INSERT operations.
[Mutations](/docs/en/sql-reference/statements/alter#mutations) and replication of tables are not supported.

A use case for this disk type are non-replicated `MergeTree` tables. Although the `s3` disk type is suitable for non-replicated
MergeTree tables, you may opt for the `s3_plain_rewritable` disk type if you do not require local metadata for the table and are
willing to accept a limited set of operations. This could be useful, for example, for system tables.

Configuration:
``` xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

is equal to
``` xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

### Using Azure Blob Storage {#azure-blob-storage}

`MergeTree` family table engines can store data to [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) using a disk with type `azure_blob_storage`.

As of February 2022, this feature is still a fresh addition, so expect that some Azure Blob Storage functionalities might be unimplemented.

Configuration markup:

``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
            </hdfs>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>

    <merge_tree>
        <min_bytes_for_wide_part>0</min_bytes_for_wide_part>
    </merge_tree>
</clickhouse>
```

Required parameters:

-   `endpoint` — HDFS endpoint URL in `path` format. Endpoint URL should contain a root path to store data.

Optional parameters:

-   `min_bytes_for_seek` — The minimal number of bytes to use seek operation instead of sequential read. Default value: `1 Mb`.

## Using Virtual File System for Data Encryption {#encrypted-virtual-file-system}

You can encrypt the data stored on [S3](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3), or [HDFS](#configuring-hdfs) external disks, or on a local disk. To turn on the encryption mode, in the configuration file you must define a disk with the type `encrypted` and choose a disk on which the data will be saved. An `encrypted` disk ciphers all written files on the fly, and when you read files from an `encrypted` disk it deciphers them automatically. So you can work with an `encrypted` disk like with a normal one.

Example of disk configuration:

``` xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

For example, when ClickHouse writes data from some table to a file `store/all_1_1_0/data.bin` to `disk1`, then in fact this file will be written to the physical disk along the path `/path1/store/all_1_1_0/data.bin`.

When writing the same file to `disk2`, it will actually be written to the physical disk at the path `/path1/path2/store/all_1_1_0/data.bin` in encrypted mode.

Required parameters:

-   `type` — `encrypted`. Otherwise the encrypted disk is not created.
-   `disk` — Type of disk for data storage.
-   `key` — The key for encryption and decryption. Type: [Uint64](../sql-reference/data-types/int-uint.md). You can use `key_hex` parameter to encrypt in hexadecimal form.
    You can specify multiple keys using the `id` attribute (see example above).

Optional parameters:

-   `path` — Path to the location on the disk where the data will be saved. If not specified, the data will be saved in the root directory.
-   `current_key_id` — The key used for encryption. All the specified keys can be used for decryption, and you can always switch to another key while maintaining access to previously encrypted data.
-   `algorithm` — [Algorithm](../sql-reference/statements/create/table.md#create-query-encryption-codecs) for encryption. Possible values: `AES_128_CTR`, `AES_192_CTR` or `AES_256_CTR`. Default value: `AES_128_CTR`. The key length depends on the algorithm: `AES_128_CTR` — 16 bytes, `AES_192_CTR` — 24 bytes, `AES_256_CTR` — 32 bytes.

Example of disk configuration:

``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

## Using local cache {#using-local-cache}

It is possible to configure local cache over disks in storage configuration starting from version 22.3. For versions 22.3 - 22.7 cache is supported only for `s3` disk type. For versions >= 22.8 cache is supported for any disk type: S3, Azure, Local, Encrypted, etc. Cache uses `LRU` cache policy.

Example of configuration for versions later or equal to 22.8:

``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
	@@ -155,7 +155,7 @@ Example of configuration for versions earlier than 22.8:
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <volumes>
                <main>
                    <disk>s3</disk>
                </main>
            </volumes>
        <policies>
    </storage_configuration>
```

Cache **configuration settings**:

- `path` - path to the directory with cache. Default: None, this setting is obligatory.

- `max_size` - maximum size of the cache in bytes or in readable format, e.g. `ki, Mi, Gi, etc`, example `10Gi` (such format works starting from `22.10` version). When the limit is reached, cache files are evicted according to the cache eviction policy. Default: None, this setting is obligatory.

- `cache_on_write_operations` - allow to turn on `write-through` cache (caching data on any write operations: `INSERT` queries, background merges). Default: `false`. The `write-through` cache can be disabled per query using setting `enable_filesystem_cache_on_write_operations` (data is cached only if both cache config settings and corresponding query setting are enabled).

- `enable_filesystem_query_cache_limit` - allow to limit the size of cache which is downloaded within each query (depends on user setting `max_query_cache_size`). Default: `false`.
- `enable_cache_hits_threshold` - a number, which defines how many times some data needs to be read before it will be cached. Default: `0`, e.g. the data is cached at the first attempt to read it.

- `do_not_evict_index_and_mark_files` - do not evict small frequently used files according to cache policy. Default: `false`. This setting was added in version 22.8. If you used filesystem cache before this version, then it will not work on versions starting from 22.8 if this setting is set to `true`. If you want to use this setting, clear old cache created before version 22.8 before upgrading.

- `max_file_segment_size` - a maximum size of a single cache file in bytes or in readable format (`ki, Mi, Gi, etc`, example `10Gi`). Default: `104857600` (`100Mi`).

- `max_elements` - a limit for a number of cache files. Default: `1048576`.

Cache **query settings**:
- `enable_filesystem_cache` - allows to disable cache per query even if storage policy was configured with `cache` disk type. Default: `true`.
- `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` - allows to use cache in query only if it already exists, otherwise query data will not be written to local cache storage. Default: `false`.
- `enable_filesystem_cache_on_write_operations` - turn on `write-through` cache. This setting works only if setting `cache_on_write_operations` in cache configuration is turned on.
- `enable_filesystem_cache_log` - turn on logging to `system.filesystem_cache_log` table. Gives a detailed view of cache usage per query. Default: `false`.
- `max_query_cache_size` - a limit for the cache size, which can be written to local cache storage. Requires enabled `enable_filesystem_query_cache_limit` in cache configuration. Default: `false`.
- `skip_download_if_exceeds_query_cache` - allows to change the behaviour of setting `max_query_cache_size`. Default: `true`. If this setting is turned on and cache download limit during query was reached, no more cache will be downloaded to cache storage. If this setting is turned off and cache download limit during query was reached, cache will still be written by cost of evicting previously downloaded (within current query) data, e.g. second behaviour allows to preserve `last recentltly used` behaviour while keeping query cache limit.
  ** Warning **
  Cache configuration settings and cache query settings correspond to the latest ClickHouse version, for earlier versions something might not be supported.
  Cache **system tables**:
- `system.filesystem_cache` - system tables which shows current state of cache.
- `system.filesystem_cache_log` - system table which shows detailed cache usage per query. Requires `enable_filesystem_cache_log` setting to be `true`.
  Cache **commands**:
- `SYSTEM DROP FILESYSTEM CACHE (<path>) (ON CLUSTER)`
- `SHOW FILESYSTEM CACHES` -- show list of filesystem caches which were configured on the server. (For versions <= `22.8` the command is named `SHOW CACHES`)
```sql
SHOW FILESYSTEM CACHES
```
Result:
``` text
┌─Caches────┐
│ s3_cache  │
└───────────┘
```
- `DESCRIBE CACHE '<cache_name>'` - show cache configuration and some general statistics for a specific cache. Cache name can be taken from `SHOW CACHES` command. (For versions <= `22.8` the command is named `DESCRIBE CACHE`)
```sql
DESCRIBE CACHE 's3_cache'
```
``` text
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─cache_on_write_operations─┬─enable_cache_hits_threshold─┬─current_size─┬─current_elements─┬─path────────┬─do_not_evict_index_and_mark_files─┐
│ 10000000000 │      1048576 │             104857600 │                         1 │                           0 │         3276 │               54 │ /s3_cache/  │                                 1 │
└─────────────┴──────────────┴───────────────────────┴───────────────────────────┴─────────────────────────────┴──────────────┴──────────────────┴─────────────┴───────────────────────────────────┘
```
Cache current metrics:
- `FilesystemCacheSize`
- `FilesystemCacheElements`
  Cache asynchronous metrics:
- `FilesystemCacheBytes`
- `FilesystemCacheFiles`
  Cache profile events:
- `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes,`
- `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds`
- `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`
- `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`

## Storing Data on Web Server {#storing-data-on-webserver}

There is a tool `clickhouse-static-files-uploader`, which prepares a data directory for a given table (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`). For each table you need, you get a directory of files. These files can be uploaded to, for example, a web server with static files. After this preparation, you can load this table into any ClickHouse server via `DiskWeb`.

This is a read-only disk. Its data is only read and never modified. A new table is loaded to this disk via `ATTACH TABLE` query (see example below). Local disk is not actually used, each `SELECT` query will result in a `http` request to fetch required data. All modification of the table data will result in an exception, i.e. the following types of queries are not allowed: [CREATE TABLE](../sql-reference/statements/create/table.md), [ALTER TABLE](../sql-reference/statements/alter/index.md), [RENAME TABLE](../sql-reference/statements/rename.md#misc_operations-rename_table), [DETACH TABLE](../sql-reference/statements/detach.md) and [TRUNCATE TABLE](../sql-reference/statements/truncate.md).

Web server storage is supported only for the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) and [Log](../engines/table-engines/log-family/log.md) engine families. To access the data stored on a `web` disk, use the [storage_policy](../engines/table-engines/mergetree-family/mergetree.md#terms) setting when executing the query. For example, `ATTACH TABLE table_web UUID '{}' (id Int32) ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy = 'web'`.

A ready test case. You need to add this configuration to config:

``` xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

And then execute this query:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

Required parameters:

-   `type` — `web`. Otherwise the disk is not created.
-   `endpoint` — The endpoint URL in `path` format. Endpoint URL must contain a root path to store data, where they were uploaded.

Optional parameters:

-   `min_bytes_for_seek` — The minimal number of bytes to use seek operation instead of sequential read. Default value: `1` Mb.
-   `remote_fs_read_backoff_threashold` — The maximum wait time when trying to read data for remote disk. Default value: `10000` seconds.
-   `remote_fs_read_backoff_max_tries` — The maximum number of attempts to read with backoff. Default value: `5`.

If a query fails with an exception `DB:Exception Unreachable URL`, then you can try to adjust the settings: [http_connection_timeout](../operations/settings/settings.md#http_connection_timeout), [http_receive_timeout](../operations/settings/settings.md#http_receive_timeout), [keep_alive_timeout](../operations/server-configuration-parameters/settings.md#keep-alive-timeout).

To get files for upload run:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` can be found in query `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

When loading files by `endpoint`, they must be loaded into `<endpoint>/store/` path, but config must contain only `endpoint`.

If URL is not reachable on disk load when the server is starting up tables, then all errors are caught. If in this case there were errors, tables can be reloaded (become visible) via `DETACH TABLE table_name` -> `ATTACH TABLE table_name`. If metadata was successfully loaded at server startup, then tables are available straight away.

Use [http_max_single_read_retries](../operations/settings/settings.md#http-max-single-read-retries) setting to limit the maximum number of retries during a single HTTP read.
