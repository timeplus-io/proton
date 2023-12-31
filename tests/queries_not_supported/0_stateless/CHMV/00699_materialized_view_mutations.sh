#!/usr/bin/env bash
# Tags: no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiquery --query="
DROP STREAM IF EXISTS view_00699;
DROP STREAM IF EXISTS null_00699;

create stream null_00699 (x uint8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW view_00699 ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM null_00699;

INSERT INTO null_00699 SELECT * FROM numbers(100);
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER STREAM null_00699 DELETE WHERE x % 2 = 0;"  --mutations_sync=1

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER STREAM view_00699 DELETE WHERE x % 2 = 0;
" --mutations_sync=1

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER STREAM null_00699 DELETE WHERE x % 2 = 1;
ALTER STREAM view_00699 DELETE WHERE x % 2 = 1;
" --mutations_sync=1

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

DROP STREAM view_00699;
DROP STREAM null_00699;
"
