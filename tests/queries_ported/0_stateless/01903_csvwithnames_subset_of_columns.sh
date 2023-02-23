#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_01903"

$CLICKHOUSE_CLIENT -q "CREATE STREAM test_01903 (col0 Date, col1 nullable(uint8)) ENGINE MergeTree() PARTITION BY to_YYYYMM(col0) ORDER BY col0;"

(echo col0,col1; for _ in `seq 1 1000000`; do echo '2021-05-05',1; done) | $CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 FORMAT CSVWithNames"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

(echo col0; for _ in `seq 1 1000000`; do echo '2021-05-05'; done) | $CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 (col0) FORMAT CSVWithNames"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

(echo col0; for _ in `seq 1 1000000`; do echo '2021-05-05'; done) | $CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 (col0) FORMAT TSVWithNames"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_01903"
