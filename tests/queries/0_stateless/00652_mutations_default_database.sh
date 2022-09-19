#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --mutations_sync=1 << EOF
DROP STREAM IF EXISTS mutations;
DROP STREAM IF EXISTS for_subquery;

create stream mutations(x uint32, y uint32) ENGINE MergeTree ORDER BY x;
INSERT INTO mutations VALUES (123, 1), (234, 2), (345, 3);

create stream for_subquery(x uint32) ENGINE TinyLog;
INSERT INTO for_subquery VALUES (234), (345);

ALTER STREAM mutations UPDATE y = y + 1 WHERE x IN for_subquery;
ALTER STREAM mutations UPDATE y = y + 1 WHERE x IN (SELECT x FROM for_subquery);
EOF

${CLICKHOUSE_CLIENT} --query="SELECT * FROM mutations"

${CLICKHOUSE_CLIENT} --query="DROP STREAM mutations"
${CLICKHOUSE_CLIENT} --query="DROP STREAM for_subquery"
