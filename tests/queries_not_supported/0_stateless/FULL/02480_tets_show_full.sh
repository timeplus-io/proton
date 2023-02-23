#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


database=$($CLICKHOUSE_CLIENT -q 'SELECT current_database()')

$CLICKHOUSE_CLIENT -nm -q "
DROP STREAM IF EXISTS test_02480_table;
DROP VIEW IF EXISTS test_02480_view;
CREATE STREAM test_02480_table (id int64) ENGINE=MergeTree ORDER BY id;
CREATE VIEW test_02480_view AS SELECT * FROM test_02480_table;
SHOW FULL STREAMS FROM $database LIKE '%';
DROP STREAM IF EXISTS test_02480_table;
DROP VIEW IF EXISTS test_02480_view;
"
