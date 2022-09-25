#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS tskv";
$CLICKHOUSE_CLIENT --query="create stream tskv (tskv_format string, timestamp datetime('UTC'), timezone string, text string, binary_data string) ";

# shellcheck disable=SC2028
echo -n 'tskv	tskv_format=custom-service-log	timestamp=2013-01-01 00:00:00	timezone=+0400	text=multiline\ntext	binary_data=can contain \0 symbol
binary_data=abc	text=Hello, world
binary_data=def	text=
tskv

' | $CLICKHOUSE_CLIENT --query="INSERT INTO tskv FORMAT TSKV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM tskv ORDER BY binary_data";
$CLICKHOUSE_CLIENT --query="DROP STREAM tskv";
