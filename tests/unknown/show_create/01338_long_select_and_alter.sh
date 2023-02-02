#!/usr/bin/env bash
# Tags: long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS alter_mt"

$CLICKHOUSE_CLIENT --query "CREATE STREAM alter_mt (key uint64, value string) ENGINE=MergeTree() ORDER BY key"

$CLICKHOUSE_CLIENT --query "INSERT INTO alter_mt SELECT number, to_string(number) FROM numbers(5)"

$CLICKHOUSE_CLIENT --query "SELECT count(distinct concat(value, '_')) FROM alter_mt WHERE not sleepEachRow(2)" &

# to be sure that select took all required locks
sleep 2

$CLICKHOUSE_CLIENT --query "ALTER STREAM alter_mt MODIFY COLUMN value uint64"

$CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM alter_mt"

wait

$CLICKHOUSE_CLIENT --query "SHOW CREATE STREAM alter_mt"

$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS alter_mt"
