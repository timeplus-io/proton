#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

EXCEPTION_TEXT="Code: 457."

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS ps";
$CLICKHOUSE_CLIENT -q "create stream ps (
    a array(uint32), da array(array(uint8)),
    t tuple(Int16, string), dt tuple(uint8, tuple(string, uint8)),
    n Nullable(date)
    ) ";

$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (
    [1, 2], [[1, 1], [2, 2]],
    (1, 'Hello'), (1, ('dt', 2)),
    NULL)";
$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (
    [10, 10, 10], [[10], [10], [10]],
    (10, 'Test'), (10, ('dt', 10)),
    '2015-02-15')";

$CLICKHOUSE_CLIENT --max_threads=1 --param_aui="[1, 2]" \
    -q "SELECT t FROM ps WHERE a = {aui:array(uint16)}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_d_a="[[1, 1], [2, 2]]" \
    -q "SELECT dt FROM ps WHERE da = {d_a:array(array(uint8))}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_tisd="(10, 'Test')" \
    -q "SELECT a FROM ps WHERE t = {tisd:tuple(Int16, string)}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_d_t="(10, ('dt', 10))" \
    -q "SELECT da FROM ps WHERE dt = {d_t:tuple(uint8, tuple(string, uint8))}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_nd="2015-02-15" \
    -q "SELECT * FROM ps WHERE n = {nd:Nullable(date)}";

# Must throw an exception to avoid SQL injection
$CLICKHOUSE_CLIENT --max_threads=1 --param_injection="[1] OR 1" \
    -q "SELECT * FROM ps WHERE a = {injection:array(uint32)}" 2>&1 \
    | grep -o "$EXCEPTION_TEXT"

$CLICKHOUSE_CLIENT -q "DROP STREAM ps";
