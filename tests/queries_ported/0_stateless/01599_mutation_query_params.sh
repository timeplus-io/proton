#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -n --query "
DROP STREAM IF EXISTS test;

CREATE STREAM test
(
  id uuid,
  date_time DateTime,
  x uint32,
  y uint32
) ENGINE = MergeTree()
PARTITION BY to_YYYYMMDD(date_time)
ORDER BY (date_time);

INSERT INTO test (x, y) VALUES (2, 1);
"

$CLICKHOUSE_CLIENT --query "SELECT x, y FROM test"

$CLICKHOUSE_CLIENT --mutations_sync 1 --param_x 1 --param_y 1 --query "
ALTER STREAM test
UPDATE x = {x:uint32}
WHERE y = {y:uint32};
"

$CLICKHOUSE_CLIENT --query "SELECT x, y FROM test"
$CLICKHOUSE_CLIENT --query "DROP STREAM test"
