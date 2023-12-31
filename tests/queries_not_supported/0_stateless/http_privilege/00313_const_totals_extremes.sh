#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT 1 AS k, count() GROUP BY k WITH TOTALS";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT 1234567890123 AS k, count() GROUP BY k WITH TOTALS FORMAT JSON";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT to_float32(1.23) AS k, count() GROUP BY k WITH TOTALS FORMAT JSONCompact";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT to_date('2010-01-01') AS k, count() GROUP BY k WITH TOTALS";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT to_datetime('2010-01-01 01:02:03') AS k, count() GROUP BY k WITH TOTALS FORMAT JSON";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1&output_format_write_statistics=0" -d "SELECT 1.1 AS k, count() GROUP BY k WITH TOTALS FORMAT JSONCompact";
