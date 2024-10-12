#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# bug: https://github.com/timeplus-io/proton/issues/838
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS datetime_csv_test"
$CLICKHOUSE_CLIENT -q "CREATE STREAM datetime_csv_test(a DateTime, b DateTime('Europe/Moscow'), c Datetime('UTC'), d Datetime64(3, 'UTC')) ENGINE=Memory()"
$CLICKHOUSE_CLIENT -q "SELECT parse_datetime_best_effort('2022-12-30 13:44:17', 'Europe/Moscow'),  to_datetime('2022-12-30 13:44:17','UTC'), parse_datetime_best_effort('2022-12-30 13:44:17','America/Los_Angeles'), to_datetime64('2022-12-30 13:44:17', 3) FORMAT CSV" > 99007_datetime_csv_test.csv
$CLICKHOUSE_CLIENT -q "INSERT INTO datetime_csv_test FORMAT CSV" < 99007_datetime_csv_test.csv
$CLICKHOUSE_CLIENT -q "SELECT * FROM datetime_csv_test"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS datetime_csv_test"
