#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS movement"

$CLICKHOUSE_CLIENT -n --query "create stream movement (date datetime('Europe/Moscow')) Engine = MergeTree ORDER BY (to_start_of_hour(date));"

$CLICKHOUSE_CLIENT --query "insert into movement select to_datetime('2020-01-22 00:00:00', 'Europe/Moscow') + number%(23*3600) from numbers(1000000);"

$CLICKHOUSE_CLIENT --query "OPTIMIZE STREAM movement FINAL"

$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) AS Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Europe/Moscow')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Europe/Moscow'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "16:00:00" | cut -f1


$CLICKHOUSE_CLIENT --query "alter stream movement delete where date >= to_datetime('2020-01-22T16:00:00', 'Europe/Moscow')  and date < to_datetime('2020-01-22T17:00:00', 'Europe/Moscow') SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) AS Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Europe/Moscow')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Europe/Moscow'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "16:00:00" | wc -l


$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) AS Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Europe/Moscow')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Europe/Moscow'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "22:00:00" | cut -f1


$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) AS Hour
FROM movement
GROUP BY Hour
ORDER BY Hour DESC
" | grep "22:00:00" | cut -f1


$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS movement"
