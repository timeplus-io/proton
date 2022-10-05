#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local query=$1
    TZ=UTC $CLICKHOUSE_CLIENT \
        --use_client_time_zone=1 \
        --input_format_values_interpret_expressions=0 \
        --query "$query" 2>/dev/null
    if [ "$?" -ne 0 ]; then
        echo "query failed"
    fi
}

perform "DROP STREAM IF EXISTS alter_00427"
perform "create stream alter_00427 (d date, a enum8('foo'=1), b datetime, c datetime) ENGINE=MergeTree(d, (a, b, to_datetime(c)), 8192)"

perform "INSERT INTO alter_00427(d, a, b, c) VALUES ('2017-02-09', 'foo', '2017-02-09 00:00:00', '2017-02-09 00:00:00')"
sleep 2
# Must fail because d is used as as a date column in MergeTree
perform "ALTER STREAM alter_00427 MODIFY COLUMN d uint16"

perform "ALTER STREAM alter_00427 MODIFY COLUMN a enum8('foo'=1, 'bar'=2)"
perform "ALTER STREAM alter_00427 MODIFY COLUMN b uint32"

# Must fail because column c is used in primary key via an expression.
perform "ALTER STREAM alter_00427 MODIFY COLUMN c uint32"
sleep 3
perform "INSERT INTO alter_00427(d, a, b, c) VALUES ('2017-02-09', 'bar', 1486598400, '2017-02-09 00:00:00')"
sleep 3
perform "SELECT d FROM alter_00427 WHERE a = 'bar'"

perform "SELECT a, b, b = to_unix_timestamp(c) FROM alter_00427 ORDER BY a FORMAT TSV"

perform "DROP STREAM alter_00427"
