#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '
SELECT
   sum(to_uint8(1) ? to_uint8(1) : to_uint8(1)) AS metric,
   group_array(to_uint8(1) ? to_uint8(1) : to_uint8(1)),
   group_array(to_uint8(1) ? to_uint8(1) : 1),
   sum(to_uint8(1) ? to_uint8(1) : 1)
FROM (SELECT materialize(to_uint64(1)) as key FROM numbers(22))
WHERE key = {b1:int64}' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&param_b1=1" -d @-
