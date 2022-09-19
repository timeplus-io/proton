#!/usr/bin/env bash
# Tags: replica, no-replicated-database
# Tag no-replicated-database: Fails due to additional replicas or shards

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --allow_nondeterministic_mutations=1 --multiquery << EOF
DROP STREAM IF EXISTS mutations_r1;
DROP STREAM IF EXISTS for_subquery;

create stream mutations_r1(x uint32, y uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mutations', 'r1') ORDER BY x;
INSERT INTO mutations_r1 VALUES (123, 1), (234, 2), (345, 3);

create stream for_subquery(x uint32) ENGINE TinyLog;
INSERT INTO for_subquery VALUES (234), (345);

ALTER STREAM mutations_r1 UPDATE y = y + 1 WHERE x IN for_subquery SETTINGS mutations_sync = 2;
ALTER STREAM mutations_r1 UPDATE y = y + 1 WHERE x IN (SELECT x FROM for_subquery) SETTINGS mutations_sync = 2;
EOF

${CLICKHOUSE_CLIENT} --query="SELECT * FROM mutations_r1"

${CLICKHOUSE_CLIENT} --query="DROP STREAM mutations_r1"
${CLICKHOUSE_CLIENT} --query="DROP STREAM for_subquery"
