#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_decimal"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM arrow_decimal (d1 decimal32(4), d2 decimal64(8), d3 decimal128(16), d4 decimal256(32)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_decimal VALUES (0.123, 0.123123123, 0.123123123123, 0.123123123123123123)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_decimal FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_decimal FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_decimal"
${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_decimal"

