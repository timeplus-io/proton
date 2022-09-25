#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --ignore-error --query="
DROP STREAM IF EXISTS test1_00550;
DROP STREAM IF EXISTS test2_00550;
DROP STREAM IF EXISTS test3_00550;

create stream test1_00550 ( id string ) ENGINE = StripeLog;
create stream test2_00550 ( id string ) ENGINE = StripeLog;
INSERT INTO test2_00550 VALUES ('a');
create stream test3_00550 ( id string, name string ) ENGINE = StripeLog;
INSERT INTO test3_00550 VALUES ('a', 'aaa');

INSERT INTO test1_00550 SELECT id, name FROM test2_00550 ANY INNER JOIN test3_00550 USING (id) SETTINGS any_join_distinct_right_table_keys=1;
INSERT INTO test1_00550 SELECT id, name FROM test2_00550 ANY LEFT OUTER JOIN test3_00550 USING (id);

DROP STREAM test1_00550;
DROP STREAM test2_00550;
DROP STREAM test3_00550;
" --server_logs_file=/dev/null 2>&1 | grep -F "Number of columns doesn't match" | wc -l

$CLICKHOUSE_CLIENT --query="SELECT 1";
