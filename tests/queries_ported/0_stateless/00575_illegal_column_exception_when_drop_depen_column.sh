#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


exception_pattern="Code: 44.*Cannot drop column \`id\`, because column \`id2\` depends on it"

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_00575;"
${CLICKHOUSE_CLIENT} --query "create stream test_00575 (dt date DEFAULT now(), id uint32, id2 uint32 DEFAULT id + 1) ENGINE = MergeTree(dt, dt, 8192);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00575(dt,id) VALUES ('2018-02-22',3), ('2018-02-22',4), ('2018-02-22',5);"
sleep 3
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_00575 ORDER BY id settings asterisk_include_reserved_columns=false;"
echo "$(${CLICKHOUSE_CLIENT} --query "ALTER STREAM test_00575 DROP COLUMN id;" --server_logs_file=/dev/null 2>&1 | grep -c "$exception_pattern")"
${CLICKHOUSE_CLIENT} --query "ALTER STREAM test_00575 DROP COLUMN id2;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_00575 ORDER BY id settings asterisk_include_reserved_columns=false;"
${CLICKHOUSE_CLIENT} --query "ALTER STREAM test_00575 DROP COLUMN id;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_00575 ORDER BY dt"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_00575;"
