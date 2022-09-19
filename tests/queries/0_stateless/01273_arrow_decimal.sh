#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_decimal"
${CLICKHOUSE_CLIENT} --query="create stream arrow_decimal (d1 Decimal32(4), d2 Decimal64(8), d3 Decimal128(16), d4 Decimal256(32)) "
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE arrow_decimal VALUES (0.123, 0.123123123, 0.123123123123, 0.123123123123123123)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_decimal FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_decimal FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_decimal"
${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_decimal"

