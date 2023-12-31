#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS movement"

$CLICKHOUSE_CLIENT -n --query "CREATE STREAM movement (date DateTime('Asia/Istanbul')) Engine = MergeTree ORDER BY (to_start_of_hour(date));"

$CLICKHOUSE_CLIENT --query "insert into movement select to_datetime('2020-01-22 00:00:00', 'Asia/Istanbul') + number%(23*3600) from numbers(1000000);"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE movement FINAL"

$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) as Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Asia/Istanbul')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Asia/Istanbul'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "16:00:00" | cut -f1


$CLICKHOUSE_CLIENT --query "alter stream movement delete where date >= to_datetime('2020-01-22T16:00:00', 'Asia/Istanbul')  and date < to_datetime('2020-01-22T17:00:00', 'Asia/Istanbul') SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) as Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Asia/Istanbul')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Asia/Istanbul'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "16:00:00" | wc -l


$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) as Hour
FROM movement
WHERE (date >= to_datetime('2020-01-22T10:00:00', 'Asia/Istanbul')) AND (date <= to_datetime('2020-01-22T23:00:00', 'Asia/Istanbul'))
GROUP BY Hour
ORDER BY Hour DESC
" | grep "22:00:00" | cut -f1


$CLICKHOUSE_CLIENT -n --query "
SELECT
    count(),
    to_start_of_hour(date) as Hour
FROM movement
GROUP BY Hour
ORDER BY Hour DESC
" | grep "22:00:00" | cut -f1


$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS movement"
