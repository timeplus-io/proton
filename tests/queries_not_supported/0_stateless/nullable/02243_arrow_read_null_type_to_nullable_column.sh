#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists test_02243"
$CLICKHOUSE_CLIENT -q "create stream test_02243 (image_path nullable(string),
                                caption nullable(string),
                                NSFW nullable(string),
                                similarity nullable(float64),
                                LICENSE nullable(string),
                                url nullable(string),
                                key nullable(uint64),
                                shard_id nullable(uint64),
                                status nullable(string),
                                error_message nullable(string),
                                width nullable(uint32),
                                height nullable(uint32),
                                exif nullable(string),
                                original_width nullable(uint32),
                                original_height nullable(uint32)) engine=Memory"

cat $CUR_DIR/data_parquet_bad_column/metadata_0.parquet | $CLICKHOUSE_CLIENT  --stacktrace -q "insert into test_02243 format Parquet"

$CLICKHOUSE_CLIENT -q "select count() from test_02243"
$CLICKHOUSE_CLIENT -q "drop stream test_02243"
