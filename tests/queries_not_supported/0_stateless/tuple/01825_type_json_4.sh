#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS t_json_4"

$CLICKHOUSE_CLIENT -q "create stream t_json_4(id uint64, data JSON) \
ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

echo '{"id": 1, "data": {"k1": "v1"}}, {"id": 2, "data": {"k1": [1, 2]}}' \
    | $CLICKHOUSE_CLIENT  -q "INSERT INTO t_json_4 FORMAT JSONEachRow" 2>&1 | grep -o -m1 "Code: 645"

echo '{"id": 1, "data": {"k1": "v1"}}, {"id": 2, "data": {"k1": [{"k2" : 1}, {"k2" : 2}]}}' \
    | $CLICKHOUSE_CLIENT  -q "INSERT INTO t_json_4 FORMAT JSONEachRow" 2>&1 | grep -o -m1 "Code: 15"

echo '{"id": 1, "data": {"k1": "v1"}}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_4 FORMAT JSONEachRow"

echo '{"id": 2, "data": {"k1": [1, 2]}}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_4 FORMAT JSONEachRow" 2>&1 | grep -o -m1 "Code: 53"

$CLICKHOUSE_CLIENT -q "SELECT id, data, to_type_name(data) FROM t_json_4"
$CLICKHOUSE_CLIENT -q "SELECT id, data.k1 FROM t_json_4 ORDER BY id"

$CLICKHOUSE_CLIENT -q "DROP STREAM t_json_4"
