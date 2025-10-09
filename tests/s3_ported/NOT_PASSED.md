
# S3 Related Tests Overview

## Currently Working Tests
- 02457_filesystem_function
- 02343_read_from_s3_compressed_blocks
- 02336_sparse_columns_s3
- 02270_errors_in_files_s3
- 02207_s3_content_type
- 02012_settings_clause_for_s3
- 01944_insert_partition_by
- 01801_s3_cluster
- 01801_s3_cluster_count

## Currently Unsupported
- 02497_remote_disk_fat_column (Does not support `allow_suspicious_fixed_string_types` setting)
- 02245_s3_schema_desc (Does not support `decode_url_component` function)
- 02432_s3_parallel_parts_cleanup
- 02494_zero_copy_projection_cancel_fetch
- 02067_lost_part_s3 (Does not support `ReplicatedMergeTree`)
- 02245_s3_support_read_nested_column (Potential bug, does not support specifying names in tuples)

Error message:
```
Code: 47. DB::Exception: Received from localhost:8463. DB::Exception: Missing columns: 'b.b' 'b.a' while processing query: 'SELECT
  a, b.a, b.b
FROM
  s3(s3_conn, filename = 'test_02245_s3_nested_parquet1_*', format = 'Parquet')', required columns: 'a' 'b.a' 'b.b', maybe you meant: ['a']. (UNKNOWN_IDENTIFIER)
(query: select a, b.a, b.b from s3(s3_conn, filename='test_02245_s3_nested_parquet1_*', format='Parquet');)
```
- 02496_storage_s3_profile_events
- 02495_s3_filter_by_file
- 02302_s3_file_pruning
- 02480_s3_support_wildcard
- 02245_s3_virtual_columns (Does not support `uint64` type in Parquet)

Query example:
```
INSERT INTO test_02245 SELECT
  1
SETTINGS
  s3_truncate_on_insert = 1
SETTINGS s3_truncate_on_insert = 1

Query id: 8a67d1eb-f9e2-49f5-87d0-6524ce187b5b
```

Error message:
```
Code: 50. DB::Exception: Received from localhost:8463. DB::Exception: Internal type 'uint64' of a column 'a' is not supported for conversion into Parquet data format.: While executing StorageS3Sink. (UNKNOWN_TYPE)
```

## Bugs
- 02344_describe_cache
- 02240_filesystem_cache_bypass_cache_threshold
- 02240_filesystem_query_cache
- 02240_system_filesystem_cache_table
- 02241_filesystem_cache_on_write_operations
- 02382_filesystem_cache_persistent_files
- 02242_system_filesystem_cache_log_table
- 02241_remote_filesystem_cache_on_insert
Syntax error example:
```
SYSTEM DROP FILESYSTEM CACHE 's3_cache/';
Expected CACHE
```

- 02494_zero_copy_projection_cancel_fetch (Token is `fetches` but still shows an error)

Error message:
```
Code: 62. DB::Exception: Syntax error: failed at position 13 ('FETCHES'): FETCHES wikistat2. Expected one of: LISTEN QUERIES, MERGES, TTL MERGES, FETCHES. (SYNTAX_ERROR)
```
- 02477_s3_request_throttler (ProfileEvents may not have been modified)
- 02457_s3_cluster_schema_inference (Potential bug, default type inferred from TSV file is `float64` instead of `int64`)

Query example:
```
SELECT
  *
FROM
  s3('http://localhost:11111/test/{a,tsv_with_header}.tsv', 'TSV', 'c1 uint64, c2 uint64, c3 uint64')
Query id: 5a553e32-087a-4fd4-b426-40fcf04afd4c
```

Result:
```
┌─c1─┬─c2─┬─c3─┐
│  1 │  2 │  3 │
│  4 │  5 │  6 │
│  7 │  8 │  9 │
│  0 │  0 │  0 │
└────┴────┴────┘
```

Error message:
```
Code: 27. DB::Exception: Received from localhost:8463. DB::ParsingException. DB::ParsingException: Cannot parse input: expected '\t' before: 'number\tnam':
Row 1:
Column 0,   name: c1, type: uint64, ERROR: text "number<TAB>nam" is not like uint64
```

## Missing Results Test Cases
- `../tests/queries/0_stateless/02226_s3_with_cache.sql`