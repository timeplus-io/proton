#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS values_floats"

${CLICKHOUSE_CLIENT} --query="CREATE STREAM values_floats (a float32, b float64) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query="SELECT '(-160.32605134916085,37.70584056842162),' FROM numbers(1000000)" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO values_floats FORMAT Values"

${CLICKHOUSE_CLIENT} --query="SELECT DISTINCT round(a, 6), round(b, 6) FROM values_floats"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS values_floats"

