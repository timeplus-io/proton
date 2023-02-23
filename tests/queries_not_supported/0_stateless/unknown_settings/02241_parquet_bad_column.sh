#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for case_insensitive in "true" "false"; do
    $CLICKHOUSE_CLIENT -q "drop stream if exists test_02241"
    $CLICKHOUSE_CLIENT -q "create stream test_02241 (image_path nullable(string),
                                    caption nullable(string),
                                    NSFW nullable(string),
                                    similarity nullable(float64),
                                    LICENSE nullable(string),
                                    url nullable(string),
                                    key nullable(uint64),
                                    shard_id nullable(uint64),
                                    status nullable(string),
                                    width nullable(uint32),
                                    height nullable(uint32),
                                    exif nullable(string),
                                    original_width nullable(uint32),
                                    original_height nullable(uint32)) engine=Memory"

    cat $CUR_DIR/data_parquet_bad_column/metadata_0.parquet | $CLICKHOUSE_CLIENT -q "insert into test_02241 SETTINGS input_format_parquet_case_insensitive_column_matching=$case_insensitive format Parquet"

    $CLICKHOUSE_CLIENT -q "select count() from test_02241"
    $CLICKHOUSE_CLIENT -q "drop stream test_02241"
done
