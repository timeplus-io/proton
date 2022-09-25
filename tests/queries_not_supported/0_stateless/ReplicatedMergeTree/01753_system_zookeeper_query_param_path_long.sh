#!/usr/bin/env bash
# Tags: long, zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test_01753";
${CLICKHOUSE_CLIENT} --query="create stream test_01753 (n int8) ENGINE=ReplicatedMergeTree('/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_01753/test', '1') ORDER BY n"

${CLICKHOUSE_CLIENT} --query="SELECT name FROM system.zookeeper WHERE path = {path:string}" --param_path "$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_01753"


${CLICKHOUSE_CLIENT} --query="DROP STREAM test_01753 SYNC";
