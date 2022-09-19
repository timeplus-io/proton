#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_window_view = 1;
DROP STREAM IF EXISTS mt;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS wv;

create stream dst(count uint64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
create stream mt(a int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv TO dst WATERMARK=STRICTLY_ASCENDING AS SELECT count(a) AS count, tumbleEnd(wid) as w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid;

INSERT INTO mt VALUES (1, '1990/01/01 12:00:00');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:01');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:02');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:10');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:11');
INSERT INTO mt VALUES (1, '1990/01/01 12:00:30');
EOF

while true; do
	$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM dst" | grep -q "3" && break || sleep .5 ||:
done

$CLICKHOUSE_CLIENT --query="SELECT * FROM dst ORDER BY w_end;"
$CLICKHOUSE_CLIENT --query="DROP STREAM wv"
$CLICKHOUSE_CLIENT --query="DROP STREAM mt"
$CLICKHOUSE_CLIENT --query="DROP STREAM dst"
