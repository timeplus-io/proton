#!/usr/bin/env bash
# Tags: race, shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT a' | wc -l
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&stacktrace=0" -d 'SELECT a' | wc -l
[[ $(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&stacktrace=1" -d 'SELECT a' | wc -l) -ge 3 ]] && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT int_div(number, 0) FROM remote('127.0.0.{2,3}', system.numbers)" | wc -l

$CLICKHOUSE_CLIENT --query="SELECT a" --server_logs_file=/dev/null 2>&1 | wc -l
[[ $($CLICKHOUSE_CLIENT --query="SELECT a" --server_logs_file=/dev/null --stacktrace 2>&1 | wc -l) -ge 3 ]] && echo 'Ok' || echo 'Fail'
$CLICKHOUSE_CLIENT --query="SELECT int_div(number, 0) FROM remote('127.0.0.{2,3}', system.numbers)" --server_logs_file=/dev/null 2>&1 | wc -l
