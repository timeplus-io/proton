#!/usr/bin/env bash
# Tags: zookeeper, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS root"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS a"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS b"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS c"

${CLICKHOUSE_CLIENT} --query "create stream root (d uint64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/root', '1') ORDER BY d"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW a (d uint64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/a', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW b (d uint64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/b', '1') ORDER BY d SETTINGS parts_to_delay_insert=1, parts_to_throw_insert=1 AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW c (d uint64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/c', '1') ORDER BY d AS SELECT * FROM root"

${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table"
if ${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (2)" 2>/dev/null; then
    echo "FAIL"
    echo "Expected 'too many parts' on table b"
fi

echo
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table, d"

${CLICKHOUSE_CLIENT} --query "DROP STREAM root"
${CLICKHOUSE_CLIENT} --query "DROP STREAM a"
${CLICKHOUSE_CLIENT} --query "DROP STREAM b"
${CLICKHOUSE_CLIENT} --query "DROP STREAM c"

# Deduplication check for non-replicated root table
echo
${CLICKHOUSE_CLIENT} --query "create stream root (d uint64) ENGINE = Null"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW d (d uint64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/d', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT * FROM d";
${CLICKHOUSE_CLIENT} --query "DROP STREAM root"
${CLICKHOUSE_CLIENT} --query "DROP STREAM d"
