#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP STREAM IF EXISTS mt;
DROP STREAM IF EXISTS wv;
DROP STREAM IF EXISTS dst;

CREATE STREAM dst(count uint64, market int64 default 0, w_end DateTime('US/Samoa')) Engine=MergeTree ORDER BY tuple();
CREATE STREAM mt(a int32, market int64, timestamp DateTime('US/Samoa')) ENGINE=MergeTree ORDER BY tuple();

CREATE WINDOW VIEW wv TO dst WATERMARK=ASCENDING AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1, 1, to_datetime('1990/01/01 12:00:00', 'US/Samoa'));
INSERT INTO mt VALUES (1, 2, to_datetime('1990/01/01 12:00:01', 'US/Samoa'));
INSERT INTO mt VALUES (1, 3, to_datetime('1990/01/01 12:00:02', 'US/Samoa'));
INSERT INTO mt VALUES (1, 4, to_datetime('1990/01/01 12:00:05', 'US/Samoa'));
INSERT INTO mt VALUES (1, 5, to_datetime('1990/01/01 12:00:06', 'US/Samoa'));
INSERT INTO mt VALUES (1, 6, to_datetime('1990/01/01 12:00:10', 'US/Samoa'));
INSERT INTO mt VALUES (1, 7, to_datetime('1990/01/01 12:00:11', 'US/Samoa'));
INSERT INTO mt VALUES (1, 8, to_datetime('1990/01/01 12:00:30', 'US/Samoa'));
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT count, market, w_end FROM dst ORDER BY w_end, market;"
$CLICKHOUSE_CLIENT --query="DROP STREAM wv;"
$CLICKHOUSE_CLIENT --query="DROP STREAM dst;"
$CLICKHOUSE_CLIENT --query="DROP STREAM mt;"
