#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP STREAM IF EXISTS mt;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS wv;

create stream dst(count uint64) Engine=MergeTree ORDER BY tuple();
create stream mt(a int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst AS SELECT count(a) AS count FROM mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '1' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1, now('US/Samoa') + 1);
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "1" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT count FROM dst;"
$CLICKHOUSE_CLIENT --query="DROP STREAM wv;"
$CLICKHOUSE_CLIENT --query="DROP STREAM mt;"
$CLICKHOUSE_CLIENT --query="DROP STREAM dst;"
