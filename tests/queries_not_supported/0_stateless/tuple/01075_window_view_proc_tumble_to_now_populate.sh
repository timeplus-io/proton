#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP STREAM IF EXISTS mt;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS wv;

CREATE STREAM dst(count uint64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE STREAM mt(a int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO mt VALUES (1);

CREATE WINDOW VIEW wv TO dst POPULATE AS SELECT count(a) AS count, tumbleEnd(wid) FROM mt GROUP BY tumble(now('US/Samoa'), INTERVAL '1' SECOND, 'US/Samoa') AS wid;
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "1" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT count FROM dst"
$CLICKHOUSE_CLIENT --query="DROP STREAM wv"
$CLICKHOUSE_CLIENT --query="DROP STREAM mt"
$CLICKHOUSE_CLIENT --query="DROP STREAM dst"
