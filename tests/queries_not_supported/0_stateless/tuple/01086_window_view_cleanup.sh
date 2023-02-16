#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 --multiquery <<EOF
SET allow_experimental_window_view = 1;
SET window_view_clean_interval = 1;

DROP DATABASE IF EXISTS test_01086;
CREATE DATABASE test_01086 ENGINE=Ordinary;

CREATE STREAM test_01086.dst(count uint64, market int32, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE STREAM test_01086.mt(a int32, market int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test_01086.wv TO test_01086.dst WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM test_01086.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid, market;

INSERT INTO test_01086.mt VALUES (1, 1, to_datetime('1990/01/01 12:00:00', 'US/Samoa'));
INSERT INTO test_01086.mt VALUES (1, 2, to_datetime('1990/01/01 12:00:01', 'US/Samoa'));
INSERT INTO test_01086.mt VALUES (1, 3, to_datetime('1990/01/01 12:00:02', 'US/Samoa'));
INSERT INTO test_01086.mt VALUES (1, 4, to_datetime('1990/01/01 12:00:05', 'US/Samoa'));
INSERT INTO test_01086.mt VALUES (1, 5, to_datetime('1990/01/01 12:00:06', 'US/Samoa'));
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM test_01086.\`.inner.wv\`" | grep -q "5" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT sleep(2);"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01086.mt VALUES (1, 6, to_datetime('1990/01/01 12:00:11', 'US/Samoa'));"

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM test_01086.\`.inner.wv\`" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT market, wid FROM test_01086.\`.inner.wv\` ORDER BY market, \`windowID(timestamp, to_intervalSecond('5'), 'US/Samoa')\` as wid";
$CLICKHOUSE_CLIENT --query="DROP STREAM test_01086.wv NO DELAY;"
$CLICKHOUSE_CLIENT --query="DROP STREAM test_01086.mt NO DELAY;"
$CLICKHOUSE_CLIENT --query="DROP STREAM test_01086.dst NO DELAY;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE test_01086 NO DELAY;"
