#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP STREAM IF EXISTS mt;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS wv;

CREATE STREAM dst(time DateTime, colA string, colB string) Engine=MergeTree ORDER BY tuple();
CREATE STREAM mt(colA string, colB string) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst AS SELECT tumbleStart(w_id) AS time, colA, colB FROM mt GROUP BY tumble(now(), INTERVAL '10' SECOND, 'US/Samoa') AS w_id, colA, colB;

INSERT INTO mt VALUES ('test1', 'test2');
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "1" && break || sleep .1 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT colA, colB FROM dst"
$CLICKHOUSE_CLIENT --query="DROP STREAM wv"
$CLICKHOUSE_CLIENT --query="DROP STREAM mt"
$CLICKHOUSE_CLIENT --query="DROP STREAM dst"
