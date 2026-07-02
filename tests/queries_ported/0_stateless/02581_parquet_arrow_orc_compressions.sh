#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='none'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='lz4'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='snappy'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='snappy', output_format_parquet_use_custom_encoder=0" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='zstd'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='brotli'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='gzip'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Parquet"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='none'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format ORC"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='lz4'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format ORC"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='zstd'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format ORC"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='zlib'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format ORC"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='snappy'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format ORC"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='none'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Arrow"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='lz4_frame'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Arrow"
$CLICKHOUSE_CLIENT -q "select count() from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(number uint64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='zstd'" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 format Arrow"
$CLICKHOUSE_CLIENT -q "select count() from table1"
