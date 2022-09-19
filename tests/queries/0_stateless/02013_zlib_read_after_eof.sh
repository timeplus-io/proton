#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on Go

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_zlib/02013_zlib_read_after_eof_data

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS graphite;"

$CLICKHOUSE_CLIENT -q "create stream graphite(\`Path\` string, \`Value\` float64, \`Time\` uint32, \`date\` date, \`Timestamp\` uint32) \
    ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY (Path, Time) SETTINGS index_granularity = 8192;"

cat "$DATA_FILE" | go run $CUR_DIR/02013_zlib_read_after_eof.go

$CLICKHOUSE_CLIENT -q "SELECT count() FROM graphite;"

$CLICKHOUSE_CLIENT -q "drop stream graphite;"
