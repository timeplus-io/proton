#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q 'DROP VIEW IF EXISTS 99021_mv;'
$CLICKHOUSE_CLIENT -q 'DROP STREAM IF EXISTS 99021_v;'

$CLICKHOUSE_CLIENT -q 'CREATE STREAM 99021_v(id int) settings shards=3;'
$CLICKHOUSE_CLIENT -q 'CREATE MATERIALIZED VIEW 99021_mv AS select * from 99021_v;'

sleep 1
$CLICKHOUSE_CLIENT -q 'INSERT INTO 99021_v(id) values(1);'
sleep 1
$CLICKHOUSE_CLIENT -q 'INSERT INTO 99021_v(id) values(2);'
sleep 1
$CLICKHOUSE_CLIENT -q 'INSERT INTO 99021_v(id) values(3);'
sleep 1

# Execute the SELECT query to get the partition information
partition=$($CLICKHOUSE_CLIENT --query="SELECT partition FROM system.parts WHERE table='99021_v' FORMAT JSON;" | jq -r '.data[0].partition')
count=$($CLICKHOUSE_CLIENT --query="SELECT partition FROM system.parts WHERE table='99021_v'" | wc -l)

# Check if a partition was found
if [ -n "$partition" ] && [ "$count" -eq 3 ]; then
    echo "Before alter, partition count is equal to 3."
    $CLICKHOUSE_CLIENT --query="ALTER STREAM 99021_v DROP PARTITION '$partition'"
else
    echo "No partition found to drop."
fi

# After dropping the partition, the underlying data files will be cleaned up in the background.
sleep 3

count=$($CLICKHOUSE_CLIENT --query="SELECT partition FROM system.parts WHERE table='99021_v'" | wc -l)
if [ "$count" -eq 0 ]; then
    echo "After alter partition, no partition found to drop."
else
    echo "No partition found to drop."
fi

$CLICKHOUSE_CLIENT -q 'DROP VIEW IF EXISTS 99021_mv;'
$CLICKHOUSE_CLIENT -q 'DROP STREAM IF EXISTS 99021_v;'
